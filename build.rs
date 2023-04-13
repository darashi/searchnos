use std::process::Command;
fn main() {
    let output = Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output()
        .expect("failed to execute git");
    let git_hash = String::from_utf8(output.stdout).expect("failed to obtain output");
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
}
