use movy_sui::compile::SuiCompiledPackage;

pub fn testing_std() -> Vec<SuiCompiledPackage> {
    let bs = include_bytes!(concat!(env!("OUT_DIR"), "/std.testing"));
    bcs::from_bytes(bs).unwrap()
}

pub fn sui_std() -> Vec<SuiCompiledPackage> {
    let bs = include_bytes!(concat!(env!("OUT_DIR"), "/std"));
    bcs::from_bytes(bs).unwrap()
}

pub fn movy() -> SuiCompiledPackage {
    let bs = include_bytes!(concat!(env!("OUT_DIR"), "/movy"));
    bcs::from_bytes(bs).unwrap()
}
