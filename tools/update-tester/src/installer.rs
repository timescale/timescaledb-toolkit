use std::path::Path;

use colored::Colorize;

use xshell::{cmd, pushd, pushenv};

use crate::{defer, quietly_run};

pub fn install_all_versions(
    root_dir: &str,
    pg_config: &str,
    old_versions: &[String],
) -> xshell::Result<()> {
    let extension_dir = path!(root_dir / "extension");
    let install_toolkit = || -> xshell::Result<()> {
        let _d = pushd(&extension_dir)?;
        let _e = pushenv("CARGO_TARGET_DIR", "../target/extension");
        quietly_run(cmd!("cargo pgx install -c {pg_config}"))
    };
    let post_install = || -> xshell::Result<()> {
        let _d = pushd(root_dir)?;
        let _e = pushenv("CARGO_TARGET_DIR", "target/top");
        quietly_run(cmd!(
            "cargo run --manifest-path ./tools/post-install/Cargo.toml -- {pg_config}"
        ))
    };

    let base_checkout = get_current_checkout()?;
    // TODO only fetch the tags we actually need
    quietly_run(cmd!("git fetch --tags"))?;
    // Install the versions in reverse-time order.
    // Since later versions tend to be supersets of old versions,
    // I expect compilation to be faster this way - Josh
    for version in old_versions.iter().rev() {
        // TODO add flag to only install versions we don't have already
        eprintln!("{} {}", "Installing".bold().cyan(), version);
        let tag_version = tag_version(version);
        quietly_run(cmd!("git checkout tags/{tag_version}"))?;
        let _d = defer(|| quietly_run(cmd!("git checkout {base_checkout}")));
        install_toolkit()?;
        post_install()?;
        eprintln!("{} {}", "Finished".bold().green(), version);
    }
    eprintln!("{} main", "Installing".bold().cyan());
    install_toolkit()?;
    post_install()?;
    eprintln!("{} main", "Finished".bold().green());

    Ok(())
}

fn get_current_checkout() -> xshell::Result<String> {
    let current_branch = cmd!("git rev-parse --abbrev-ref --symbolic-full-name HEAD").read()?;

    if current_branch != "HEAD" {
        return Ok(current_branch);
    }

    cmd!("git rev-parse --verify HEAD").read()
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
