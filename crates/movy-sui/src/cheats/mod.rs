use std::{str::FromStr, sync::LazyLock};

use move_core_types::account_address::AccountAddress;
use move_vm_runtime::native_functions::NativeFunctionTable;
use sui_types::Identifier;

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

pub fn cheat_address() -> &'static AccountAddress {
    static CHEAT: LazyLock<AccountAddress> =
        LazyLock::new(|| AccountAddress::from_str("0xdeadbeef").unwrap());
    &CHEAT
}

pub fn all_cheates() -> NativeFunctionTable {
    vec![make_cheat!(
        "cheats",
        "new_tx_context",
        super::cheats::ctx::new_tx_context
    )]
}
