use std::{collections::HashSet, path::Path};

use colored::Colorize;
use semver::Version;
use toml_edit::Document;
use xshell::{cmd, cp, mkdir_p, pushd, read_dir};

use crate::{defer, quietly_run};

fn pgrx_name(version: &Version) -> &'static str {
    if version >= &Version::new(0, 7, 4) {
        "pgx"
    } else {
        "pgrx"
    }
}

#[allow(clippy::too_many_arguments)]
pub fn install_all_versions(
    root_dir: &str,
    cache_dir: Option<&str>,
    pg_config: &str,
    cargo_pgrx: &str,
    cargo_pgrx_old: &str,
    current_version: &str,
    old_versions: &[String],
    reinstall: &HashSet<&str>,
) -> xshell::Result<()> {
    let extension_dir = path!(root_dir / "extension");
    let install_toolkit = |pgrx_version: Version| -> xshell::Result<()> {
        let _d = pushd(&extension_dir)?;
        let pgrx_name = pgrx_name(&pgrx_version);
        match pgrx_version >= Version::new(0, 4, 0) {
            true => quietly_run(cmd!("{cargo_pgrx} {pgrx_name} install -c {pg_config}")),
            false => quietly_run(cmd!("{cargo_pgrx_old} {pgrx_name} install -c {pg_config}")),
        }
    };
    let post_install = || -> xshell::Result<()> {
        let _d = pushd(root_dir)?;
        quietly_run(cmd!(
            "cargo run --manifest-path ./tools/post-install/Cargo.toml -- {pg_config}"
        ))
    };

    if let Some(cache_dir) = cache_dir {
        restore_from_cache(cache_dir, pg_config)?
    }

    {
        let base_checkout = get_current_checkout()?;
        let pgrx_version = get_pgrx_version(
            &std::fs::read_to_string("extension/Cargo.toml").expect("unable to read Cargo.toml"),
        );
        // Install the versions in reverse-time order.
        // Since later versions tend to be supersets of old versions,
        // I expect compilation to be faster this way - Josh
        for version in old_versions.iter().rev() {
            let force_reinstall = reinstall.contains(&**version);
            if !force_reinstall && version_is_installed(pg_config, version)? {
                eprintln!("{} {}", "Already Installed".blue(), version);
                continue;
            }
            eprintln!("{} {}", "Installing".bold().cyan(), version);
            let tag_version = tag_version(version);
            quietly_run(cmd!("git fetch origin tag {tag_version}"))?;
            quietly_run(cmd!("git checkout tags/{tag_version}"))?;
            let _d = defer(|| quietly_run(cmd!("git checkout {base_checkout}")));
            let pgrx_version = get_pgrx_version(
                &std::fs::read_to_string("extension/Cargo.toml")
                    .expect("unable to read Cargo.toml"),
            );
            install_toolkit(pgrx_version)?;
            post_install()?;
            eprintln!("{} {}", "Finished".bold().green(), version);
        }

        if let Some(cache_dir) = cache_dir {
            save_to_cache(cache_dir, pg_config)?;
        }

        eprintln!(
            "{} {} ({})",
            "Installing Current".bold().cyan(),
            current_version,
            base_checkout
        );
        install_toolkit(pgrx_version)?;
    }
    post_install()?;
    eprintln!("{}", "Finished Current".bold().green());

    Ok(())
}

fn get_current_checkout() -> xshell::Result<String> {
    let current_branch = cmd!("git rev-parse --abbrev-ref --symbolic-full-name HEAD").read()?;

    if current_branch != "HEAD" {
        return Ok(current_branch);
    }

    cmd!("git rev-parse --verify HEAD").read()
}

fn get_pgrx_version(cargo_toml_contents: &str) -> Version {
    let cargo = cargo_toml_contents
        .parse::<Document>()
        .expect("invalid Cargo.toml");

    let pgrx_dependency = cargo["dependencies"]
        .get("pgrx")
        // check old name if no pgrx found
        .unwrap_or_else(|| &cargo["dependencies"]["pgx"]);
    pgrx_dependency
        .as_str()
        .expect("expected pgrx to only have a version")
        .trim_start_matches(['=', '^', '~'].as_slice())
        .parse()
        .expect("cannot parse pgrx version")
}

// We were unprincipled with some of our old versions, so the version from
// the control file is `x.y`, while the tag is `x.y.0`. This function translates
// from the control file version to the tag version (in a rather hacky way)
fn tag_version(version: &str) -> String {
    if version.matches('.').count() >= 2 {
        return version.into();
    }

    format!("{}.0", version)
}

//-----------------------//
//-- Cache Maintenance --//
//-----------------------//

fn version_is_installed(pg_config: &str, version: &str) -> xshell::Result<bool> {
    let binary_name = format!("timescaledb_toolkit-{}.so", version);
    let bin_dir = cmd!("{pg_config} --pkglibdir").read()?;
    let installed_files = read_dir(bin_dir)?;
    let installed = installed_files.into_iter().any(|file| {
        file.file_name()
            .map(|name| name.to_string_lossy() == binary_name)
            .unwrap_or(false)
    });
    Ok(installed)
}

fn restore_from_cache(cache_dir: &str, pg_config: &str) -> xshell::Result<()> {
    if !path!(cache_dir).exists() {
        eprintln!("{}", "Cache does not exist".yellow());
        return Ok(());
    }

    eprintln!("{} {}", "Restoring from Cache".bold().blue(), cache_dir);
    let bin_dir = cmd!("{pg_config} --pkglibdir").read()?;

    let share_dir = cmd!("{pg_config} --sharedir").read()?;
    let script_dir = path!(share_dir / "extension");

    let cached_bin_dir = path!(cache_dir / "bin");
    let cached_script_dir = path!(cache_dir / "extension");

    cp_dir(cached_bin_dir, bin_dir, |_| true)?;
    cp_dir(cached_script_dir, script_dir, |_| true)
}

fn save_to_cache(cache_dir: &str, pg_config: &str) -> xshell::Result<()> {
    eprintln!("{} {}", "Saving to Cache".blue(), cache_dir);

    let cached_bin_dir = path!(cache_dir / "bin");
    let cached_script_dir = path!(cache_dir / "extension");

    if !cached_bin_dir.exists() {
        mkdir_p(&cached_bin_dir)?
    }

    if !cached_script_dir.exists() {
        mkdir_p(&cached_script_dir)?
    }

    let bin_dir = cmd!("{pg_config} --pkglibdir").read()?;

    let share_dir = cmd!("{pg_config} --sharedir").read()?;
    let script_dir = path!(share_dir / "extension");

    let is_toolkit_file = |file: &Path| {
        file.file_name()
            .map(|f| f.to_string_lossy().starts_with("timescaledb_toolkit"))
            .unwrap_or(false)
    };
    cp_dir(bin_dir, cached_bin_dir, is_toolkit_file)?;
    cp_dir(script_dir, cached_script_dir, is_toolkit_file)
}

fn cp_dir(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    mut filter: impl FnMut(&Path) -> bool,
) -> xshell::Result<()> {
    let dst = dst.as_ref();
    for file in read_dir(src)? {
        if filter(&file) {
            cp(file, dst)?;
        }
    }
    Ok(())
}
