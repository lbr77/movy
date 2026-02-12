use std::{str::FromStr, sync::LazyLock};

use move_core_types::account_address::AccountAddress;
use move_vm_runtime::native_functions::NativeFunctionTable;
use sui_types::Identifier;

use crate::{cheats::backend::CheatBackend, database::cache::CachedSnapshot};

pub mod backend;
pub mod ctx;
pub mod scenario;

macro_rules! make_cheat {
    ($mod:literal, $func:literal, $native: expr) => {
        (
            cheat_address().clone(),
            Identifier::new($mod).unwrap(),
            Identifier::new($func).unwrap(),
            std::sync::Arc::new(
                move |context,
                      ty_args,
                      args|
                      -> move_binary_format::errors::PartialVMResult<
                    move_vm_types::natives::function::NativeResult,
                > { $native(context, ty_args, args) },
            ),
        )
    };
}

macro_rules! make_backend_cheat {
    ($backend:expr, $mod:literal, $func:literal, $native: expr) => {
        (
            cheat_address().clone(),
            Identifier::new($mod).unwrap(),
            Identifier::new($func).unwrap(),
            {
                let backend = $backend.clone();
                std::sync::Arc::new(
                    move |context,
                          ty_args,
                          args|
                          -> move_binary_format::errors::PartialVMResult<
                        move_vm_types::natives::function::NativeResult,
                    > { $native(&backend, context, ty_args, args) },
                )
            },
        )
    };
}

pub fn cheat_address() -> &'static AccountAddress {
    static CHEAT: LazyLock<AccountAddress> =
        LazyLock::new(|| AccountAddress::from_str("0xdeadbeef").unwrap());
    &CHEAT
}

pub fn all_cheates(storage: CachedSnapshot) -> (CheatBackend, NativeFunctionTable) {
    let backend = CheatBackend::new(storage);
    (
        backend.clone(),
        vec![
            make_cheat!(
                "cheats",
                "new_tx_context",
                super::cheats::ctx::new_tx_context
            ),
            make_backend_cheat!(
                backend,
                "cheats",
                "share_object_impl",
                super::cheats::scenario::share_object_impl
            ),
            make_backend_cheat!(
                backend,
                "cheats",
                "end_transaction",
                super::cheats::scenario::end_transaction
            ),
        ],
    )
}
