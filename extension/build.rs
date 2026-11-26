// Git info for get_toolkit_git_commit(). No git means NULL.

use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

fn main() {
    // Rebuild on commit and checkout
    if let Some(dir) = git(&["rev-parse", "--absolute-git-dir"]) {
        println!("cargo:rerun-if-changed={dir}/logs/HEAD");
    }
    if let Some(tag) = git(&["describe", "--dirty", "--always", "--tags"]) {
        println!("cargo:rustc-env=TOOLKIT_GIT_COMMIT_TAG={tag}");
    }
    if let Some(hash) = git(&["rev-parse", "HEAD"]) {
        println!("cargo:rustc-env=TOOLKIT_GIT_COMMIT_HASH={hash}");
    }
}
