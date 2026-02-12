use std::collections::VecDeque;

use move_binary_format::errors::{PartialVMError, PartialVMResult};
use move_core_types::{
    account_address::AccountAddress, gas_algebra::InternalGas, vm_status::StatusCode,
};
use move_vm_runtime::native_functions::NativeContext;
use move_vm_types::{
    loaded_data::runtime_types::Type, natives::function::NativeResult, values::Value,
};
use sui_move_natives_latest::get_nth_struct_field;
use sui_types::{
    TypeTag,
    base_types::{MoveObjectType, ObjectID},
};

use crate::cheats::backend::CheatBackend;

fn get_specified_ty(mut ty_args: Vec<Type>) -> Type {
    assert!(ty_args.len() == 1);
    ty_args.pop().unwrap()
}

fn pop_id(args: &mut VecDeque<Value>) -> PartialVMResult<ObjectID> {
    let v = match args.pop_back() {
        None => {
            return Err(PartialVMError::new(
                StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
            ));
        }
        Some(v) => v,
    };
    Ok(get_nth_struct_field(v, 0)?
        .value_as::<AccountAddress>()?
        .into())
}

fn object_type_of_type(context: &NativeContext, ty: &Type) -> PartialVMResult<MoveObjectType> {
    let TypeTag::Struct(s_tag) = context.type_to_type_tag(ty)? else {
        return Err(PartialVMError::new(
            StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
        ));
    };
    Ok(MoveObjectType::from(*s_tag))
}

// native fun was_taken_shared(id: ID): bool;
// native fun most_recent_id_shared<T: key>(): Option<ID>;

// // native fun objects_by_type<T: key>(): vector<ID>;

// // Forward to sui std native call
// native fun share_object_impl<T: key>(obj: T);
pub fn share_object_impl(
    _backend: &CheatBackend,
    ctx: &mut NativeContext,
    tys: Vec<Type>,
    vals: VecDeque<Value>,
) -> PartialVMResult<NativeResult> {
    sui_move_natives_latest::transfer::share_object(ctx, tys, vals)
}

// // native fun freeze_object_impl<T: key>(obj: T); // TODO

// native fun take_by_id<T: key>(id: ID): T;
pub fn take_by_id(
    backend: &CheatBackend,
    ctx: &mut NativeContext,
    tys: Vec<Type>,
    mut vals: VecDeque<Value>,
) -> PartialVMResult<NativeResult> {
    let ty = get_specified_ty(tys);
    let id = pop_id(vals)?;
    let specified_obj_ty = object_type_of_type(ctx, &ty)?;
    // TODO
    PartialVMResult::Ok(NativeResult::ok(InternalGas::zero(), vec![].into()))
}

// native fun end_transaction();
pub fn end_transaction(
    backend: &CheatBackend,
    _ctx: &mut NativeContext,
    _tys: Vec<Type>,
    _vals: VecDeque<Value>,
) -> PartialVMResult<NativeResult> {
    PartialVMResult::Ok(NativeResult::ok(InternalGas::zero(), vec![].into()))
}
