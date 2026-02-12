use std::collections::VecDeque;

use move_binary_format::errors::PartialVMResult;
use move_core_types::gas_algebra::InternalGas;
use move_vm_runtime::native_functions::NativeContext;
use move_vm_types::{
    loaded_data::runtime_types::Type, natives::function::NativeResult, values::Value,
};
use sui_types::storage::ObjectStore;

use crate::cheats::backend::CheatBackend;

// pub fn take_by_id<T>(db: T, _ctx: &NativeContext, tys: Vec<Type>, vals: VecDeque<Value>) -> PartialVMResult<NativeResult>
// {

// }

// native fun end_transaction();
pub fn end_transaction<T>(
    backend: &CheatBackend<T>,
    _ctx: &NativeContext,
    tys: Vec<Type>,
    vals: VecDeque<Value>,
) -> PartialVMResult<NativeResult> {
    backend.inner_mut().end_transaction();
    PartialVMResult::Ok(NativeResult::ok(InternalGas::zero(), vec![].into()))
}
