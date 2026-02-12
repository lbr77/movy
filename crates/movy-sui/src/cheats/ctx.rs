// use std::collections::VecDeque;

// use move_binary_format::errors::{PartialVMError, PartialVMResult};
// use move_vm_runtime::native_functions::NativeContext;
// use move_vm_types::{
//     loaded_data::runtime_types::Type,
//     natives::function::{InternalGas, NativeResult, StatusCode},
//     values::{Struct, Value},
// };
// use tracing::instrument;

// // public native fun new_tx_context(
// // sender: address,
// // tx_hash: vector<u8>,
// // epoch: u64,
// // epoch_timestamp_ms: u64,
// // ids_created: u64): TxContext;
// #[instrument(skip(_ctx))]
// pub fn new_tx_context(
//     _ctx: &mut NativeContext,
//     _tys: Vec<Type>,
//     vals: VecDeque<Value>,
// ) -> PartialVMResult<NativeResult> {
//     if vals.len() != 5 {
//         return PartialVMResult::Err(PartialVMError::new(StatusCode::ABORT_TYPE_MISMATCH_ERROR));
//     }
//     tracing::debug!("new_tx_context");
//     let tx_context = Value::struct_(Struct::pack(vals));
//     PartialVMResult::Ok(NativeResult::ok(InternalGas::zero(), [tx_context].into()))
// }
