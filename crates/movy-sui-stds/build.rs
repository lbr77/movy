use movy_sui::compile::{SuiCompiledPackage, build_package_resolved};
use serde::Serialize;
use std::{
    io::Write,
    path::{Path, PathBuf},
};

macro_rules! cargo_print {
    ($($tokens: tt)*) => {
        println!("cargo:warning={}", format!($($tokens)*))
    }
}

fn clear_build(dir: &Path) {
    let previous_build = dir.join("build");
    if previous_build.exists() {
        std::fs::remove_dir_all(&previous_build).unwrap();
    }
}

fn build_movy(dir: &Path) -> SuiCompiledPackage {
    clear_build(dir);
    SuiCompiledPackage::build_all_unpublished_from_folder(dir, false).unwrap()
}

fn build_std(dir: &Path, test: bool) -> Vec<SuiCompiledPackage> {
    clear_build(dir);
    let (_, resolved) = build_package_resolved(dir, test).unwrap();
    let flag = if test { "testing" } else { "non-testing" };
    let mut deps = vec![];
    for (package_name, package) in resolved.package_table.iter() {
        if package_name.as_str() != "hello_std" {
            cargo_print!(
                "Building {} std {} at {}",
                flag,
                package_name.as_str(),
                package.package_path.display()
            );

            let out =
                SuiCompiledPackage::build_all_unpublished_from_folder(&package.package_path, test)
                    .unwrap();
            let build_directory = package.package_path.join("build");
            if build_directory.exists() {
                std::fs::remove_dir_all(&build_directory).unwrap();
            }
            deps.push(out);
        }
    }
    deps
}

fn write_bcs<T: Serialize>(path: &Path, val: T) {
    let mut fp = std::fs::File::create(path).unwrap();
    let bytes = bcs::to_bytes(&val).unwrap();
    fp.write_all(&bytes).unwrap();
}

fn main() {
    let std = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/hello_std"));
    let movy = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/movy"));
    println!("cargo::rerun-if-changed={}", std.join("sources").display());
    println!("cargo::rerun-if-changed={}", movy.join("sources").display());
    println!(
        "cargo::rerun-if-changed={}",
        std.join("Move.toml").display()
    );
    println!(
        "cargo::rerun-if-changed={}",
        movy.join("Move.toml").display()
    );

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let movy = build_movy(&movy);
    let testing_stds = build_std(&std, true);
    let non_testing_std = build_std(&std, false);

    write_bcs(&out_dir.join("std.testing"), &testing_stds);
    write_bcs(&out_dir.join("std"), &non_testing_std);
    write_bcs(&out_dir.join("movy"), &movy);
}
