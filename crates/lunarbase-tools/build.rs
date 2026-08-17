//! Embeds the exact toolchain identity used to build performance binaries.

use std::ffi::{OsStr, OsString};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-env-changed=CARGO");
    emit("LUNARBASE_BUILD_TARGET", std::env::var("TARGET").ok());
    emit("LUNARBASE_BUILD_PROFILE", std::env::var("PROFILE").ok());
    emit(
        "LUNARBASE_BUILD_RUSTC_VERSION",
        command_version(std::env::var_os("RUSTC"), "rustc"),
    );
    emit(
        "LUNARBASE_BUILD_CARGO_VERSION",
        command_version(std::env::var_os("CARGO"), "cargo"),
    );
}

fn command_version(configured: Option<OsString>, fallback: &str) -> Option<String> {
    let executable = configured
        .as_deref()
        .unwrap_or_else(|| OsStr::new(fallback));
    let output = Command::new(executable).arg("-vV").output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
}

fn emit(name: &str, value: Option<String>) {
    println!(
        "cargo:rustc-env={name}={}",
        value.as_deref().unwrap_or("unknown")
    );
}
