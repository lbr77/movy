use move_binary_format::{CompiledModule, file_format::Bytecode};
use movy_types::abi::MoveModuleId;
use sui_types::{base_types::ObjectID, storage::BackingPackageStore};

use crate::tracer::fuzz::{PackageResolvedCache, PackageResolver};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InstructionExtraInformation {
    Pack(usize),
    PackGeneric(usize),
    PackVariant(usize),
    PackVariantGeneric(usize),
    Unpack(usize),
    UnpackVariant(usize),
    UnpackGeneric(usize),
    UnpackVariantGeneric(usize),
}

impl InstructionExtraInformation {
    pub fn from_resolver(
        instruction: &Bytecode,
        resolver: &PackageResolvedCache,
        package_id: &ObjectID,
        module_id: &MoveModuleId,
    ) -> Option<InstructionExtraInformation> {
        use move_binary_format::file_format::Bytecode as B;
        use move_binary_format::file_format::StructFieldInformation;

        let mut extra = None;
        match instruction {
            B::Unpack(sidx) => {
                let module = resolver.module_ref(module_id, package_id)?;
                let struct_def = module.struct_def_at(*sidx);
                let field_count = match &struct_def.field_information {
                    StructFieldInformation::Native => 0,
                    StructFieldInformation::Declared(fields) => fields.len(),
                };
                extra = Some(InstructionExtraInformation::Unpack(field_count as usize));
            }
            B::UnpackVariant(vidx)
            | B::UnpackVariantImmRef(vidx)
            | B::UnpackVariantMutRef(vidx) => {
                let module = resolver.module_ref(module_id, package_id)?;
                let variant_handle = module.variant_handle_at(*vidx);
                let variant_def =
                    module.variant_def_at(variant_handle.enum_def, variant_handle.variant);
                let field_count = variant_def.fields.len();
                extra = Some(InstructionExtraInformation::UnpackVariant(
                    field_count as usize,
                ));
            }
            B::UnpackGeneric(sidx) => {
                let module = resolver.module_ref(module_id, package_id)?;
                let struct_inst = module.struct_instantiation_at(*sidx);
                let struct_def = module.struct_def_at(struct_inst.def);
                let field_count = match &struct_def.field_information {
                    StructFieldInformation::Native => 0,
                    StructFieldInformation::Declared(fields) => fields.len(),
                };
                extra = Some(InstructionExtraInformation::UnpackGeneric(
                    field_count as usize,
                ));
            }
            B::UnpackVariantGeneric(vidx)
            | B::UnpackVariantGenericImmRef(vidx)
            | B::UnpackVariantGenericMutRef(vidx) => {
                let module = resolver.module_ref(module_id, package_id)?;
                let variant_inst_handle = module.variant_instantiation_handle_at(*vidx);
                let enum_inst = module.enum_instantiation_at(variant_inst_handle.enum_def);
                let variant_def = module.variant_def_at(enum_inst.def, variant_inst_handle.variant);
                let field_count = variant_def.fields.len();
                extra = Some(InstructionExtraInformation::UnpackVariantGeneric(
                    field_count as usize,
                ));
            }
            B::Pack(sidx) => {
                let module = resolver.module_ref(module_id, package_id)?;
                let struct_def = module.struct_def_at(*sidx);
                let field_count = match &struct_def.field_information {
                    StructFieldInformation::Native => 0,
                    StructFieldInformation::Declared(fields) => fields.len(),
                };
                extra = Some(InstructionExtraInformation::Pack(field_count as usize));
            }
            B::PackGeneric(sidx) => {
                let module = resolver.module_ref(module_id, package_id)?;
                let struct_inst = module.struct_instantiation_at(*sidx);
                let struct_def = module.struct_def_at(struct_inst.def);
                let field_count = match &struct_def.field_information {
                    StructFieldInformation::Native => 0,
                    StructFieldInformation::Declared(fields) => fields.len(),
                };
                extra = Some(InstructionExtraInformation::PackGeneric(
                    field_count as usize,
                ));
            }
            B::PackVariant(vidx) => {
                let module = resolver.module_ref(module_id, package_id)?;
                let variant_handle = module.variant_handle_at(*vidx);
                let variant_def =
                    module.variant_def_at(variant_handle.enum_def, variant_handle.variant);
                let field_count = variant_def.fields.len();
                extra = Some(InstructionExtraInformation::PackVariant(
                    field_count as usize,
                ));
            }
            B::PackVariantGeneric(vidx) => {
                let module = resolver.module_ref(module_id, package_id)?;
                let variant_inst_handle = module.variant_instantiation_handle_at(*vidx);
                let enum_inst = module.enum_instantiation_at(variant_inst_handle.enum_def);
                let variant_def = module.variant_def_at(enum_inst.def, variant_inst_handle.variant);
                let field_count = variant_def.fields.len();
                extra = Some(InstructionExtraInformation::PackVariantGeneric(
                    field_count as usize,
                ));
            }
            _ => {}
        }
        extra
    }
}
