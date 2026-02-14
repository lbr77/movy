use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    str::FromStr,
};

use color_eyre::eyre::eyre;
use itertools::Itertools;
use move_binary_format::CompiledModule;
use movy_sui::{
    compile::SuiCompiledPackage,
    database::cache::ObjectSuiStoreCommit,
    rpc::graphql::{GraphQlClient, OwnerKind},
};
use movy_types::{
    abi::{MOVY_INIT, MovePackageAbi},
    error::MovyError,
    input::{MoveAddress, MoveStructTag},
};
use sui_types::{
    Identifier,
    base_types::{ObjectID, SequenceNumber},
    digests::TransactionDigest,
    effects::TransactionEffectsAPI,
    move_package::MovePackage,
    object::{Data, Object},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    storage::{BackingPackageStore, BackingStore, ObjectStore},
};

use crate::{
    db::{ObjectStoreCachedStore, ObjectStoreInfo},
    exec::SuiExecutor,
    tracer::{NopTracer, tree::TreeTracer},
};

pub struct SuiTestingEnv<T> {
    db: T,
}

fn record_zero_address_modules(
    module_addr_map: &mut BTreeMap<String, ObjectID>,
    modules: &[CompiledModule],
    package_addr: ObjectID,
) -> Result<(), MovyError> {
    for module in modules.iter() {
        let name = module.name().to_string();
        if let Some(prev) = module_addr_map.get(&name).copied() {
            if prev != package_addr {
                tracing::debug!(
                    "duplicate module name {} mapped to both {} and {}, keep {}",
                    name,
                    prev,
                    package_addr,
                    prev
                );
            }
            continue;
        }
        module_addr_map.insert(name, package_addr);
    }
    Ok(())
}

impl<T> SuiTestingEnv<T> {
    pub fn inner(&self) -> &T {
        &self.db
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.db
    }
    pub fn into_inner(self) -> T {
        self.db
    }
}

impl<
    T: ObjectStoreCachedStore
        + ObjectStoreInfo
        + ObjectStore
        + ObjectSuiStoreCommit
        + BackingStore
        + BackingPackageStore
        + Clone
        + 'static,
> SuiTestingEnv<T>
{
    pub fn new(db: T) -> Self {
        Self { db }
    }

    pub fn install_movy(&self) -> Result<(), MovyError> {
        let movy = movy_sui_stds::movy();
        tracing::info!("Installing movy to {}", movy.package_id);
        let (modules, deps) = movy.into_deployment();
        let movy_package = Object::new_package_from_data(
            Data::Package(MovePackage::new_system(
                SequenceNumber::new(),
                &modules,
                deps,
            )),
            TransactionDigest::genesis_marker(),
        );
        self.db.commit_single_object(movy_package)?;
        Ok(())
    }

    pub fn install_std(&self, test: bool) -> Result<(), MovyError> {
        // This is pretty hacky but works
        let stds = if test {
            movy_sui_stds::testing_std()
        } else {
            movy_sui_stds::sui_std()
        };

        let flag = if test { "testing" } else { "non-testing" };
        for out in stds {
            let out = out.movy_mock()?;
            if out.package_id != ObjectID::ZERO {
                tracing::info!("Committing {} std {}", flag, out.package_id);
                tracing::debug!(
                    "Modules are {}",
                    out.all_modules_iter()
                        .map(|v| v.self_id().name().to_string())
                        .join(",")
                );
                // let std_onchain_version = self
                //     .db
                //     .get_object(&out.package_id)
                //     .ok_or_else(|| eyre!("{} not onchain?!", out.package_id))?
                //     .version();
                let (modules, dependencies) = out.into_deployment();
                let move_package = Object::new_system_package(
                    &modules,
                    SequenceNumber::from_u64(0xff),
                    dependencies,
                    TransactionDigest::genesis_marker(),
                );
                self.db.commit_single_object(move_package)?;
            }
        }

        Ok(())
    }

    pub fn install_non_testing_std(&self) -> Result<(), MovyError> {
        self.install_std(false)
    }

    pub fn mock_testing_std(&self) -> Result<(), MovyError> {
        self.install_std(true)
    }

    pub async fn load_local(
        &self,
        path: &Path,
        deployer: MoveAddress,
        attacker: MoveAddress,
        epoch: u64,
        epoch_ms: u64,
        gas: ObjectID,
        trace_movy_init: bool,
    ) -> Result<
        (
            MoveAddress,
            MovePackageAbi,
            MovePackageAbi,
            Vec<String>,
            BTreeMap<String, MoveAddress>,
        ),
        MovyError,
    > {
        tracing::info!("Compiling {} with test mode...", path.display());
        let mut compiled_result =
            SuiCompiledPackage::build_all_unpublished_from_folder(path, true)?;
        compiled_result.ensure_immediate_deps();
        let root_package_name = compiled_result.package_name.clone();
        let package_names = compiled_result.package_names.clone();
        let mut package_name_map: BTreeMap<String, MoveAddress> = BTreeMap::new();
        for (dep_name, dep_id) in compiled_result.published_dep_ids() {
            package_name_map
                .entry(dep_name.clone())
                .or_insert((*dep_id).into());
            package_name_map
                .entry(dep_name.to_ascii_lowercase())
                .or_insert((*dep_id).into());
        }
        let mut non_test_abi = compiled_result.abi()?;
        let mut executor = SuiExecutor::new(self.db.clone())?;

        // Redeploy all local dependencies in dependency-first order before root package deploy.
        // `unpublished_dep_order` includes the reversed topological order from compile phase.
        let mut zero_module_addr_map: BTreeMap<String, ObjectID> = BTreeMap::new();
        let mut package_id_map: BTreeMap<ObjectID, ObjectID> = BTreeMap::new();
        tracing::debug!(
            "published dep ids keys: {}",
            compiled_result
                .published_dep_ids()
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        );
        for dep_name in compiled_result.unpublished_dep_order().iter() {
            let Some(modules) = compiled_result.unpublished_dep_modules().get(dep_name) else {
                return Err(eyre!("missing modules for dependency {}", dep_name).into());
            };
            if modules.is_empty() {
                return Err(eyre!("empty modules for dependency {}", dep_name).into());
            }

            let dep_self_addr = ObjectID::from(*modules[0].address());
            let dep_target_addr = compiled_result
                .published_dep_ids()
                .get(dep_name)
                .copied()
                .unwrap_or(dep_self_addr);
            tracing::debug!(
                "dep {} self {} target {}",
                dep_name,
                dep_self_addr,
                dep_target_addr
            );

            let mut dep_pkg =
                SuiCompiledPackage::new_unpublished(dep_name.clone(), modules.clone());
            if !package_id_map.is_empty() {
                dep_pkg.rewrite_deps_by_package_id(&package_id_map)?;
            }
            if dep_self_addr == ObjectID::ZERO && !zero_module_addr_map.is_empty() {
                dep_pkg.rewrite_deps_by_module_name(&zero_module_addr_map)?;
            }
            dep_pkg.ensure_immediate_deps();
            if !package_id_map.is_empty() {
                dep_pkg.rewrite_deps_by_package_id(&package_id_map)?;
                dep_pkg.ensure_immediate_deps();
            }
            let dep_pkg = dep_pkg.movy_mock()?;
            let dep_address = if dep_target_addr == ObjectID::ZERO {
                executor.deploy_contract(epoch, epoch_ms, deployer.into(), gas, dep_pkg)?
            } else {
                executor.force_deploy_contract_at(dep_target_addr, dep_pkg)?
            };
            tracing::info!("publishing {} at {}", dep_name, dep_address);
            package_name_map.insert(dep_name.clone(), dep_address.into());
            package_name_map.insert(dep_name.to_ascii_lowercase(), dep_address.into());

            if dep_self_addr != ObjectID::ZERO && dep_self_addr != dep_address {
                package_id_map.insert(dep_self_addr, dep_address);
            }
            if dep_target_addr != ObjectID::ZERO && dep_target_addr != dep_address {
                package_id_map.insert(dep_target_addr, dep_address);
            }

            // Record module -> package mapping for all deployed dependencies so later packages
            // can rewrite zero-address module handles to concrete on-chain package IDs.
            record_zero_address_modules(&mut zero_module_addr_map, modules, dep_address)?;
        }

        if !package_id_map.is_empty() {
            compiled_result.rewrite_deps_by_package_id(&package_id_map)?;
        }
        if !zero_module_addr_map.is_empty() {
            compiled_result.rewrite_deps_by_module_name(&zero_module_addr_map)?;
        }
        compiled_result.ensure_immediate_deps();
        if !package_id_map.is_empty() {
            compiled_result.rewrite_deps_by_package_id(&package_id_map)?;
            compiled_result.ensure_immediate_deps();
        }

        let compiled_result = compiled_result.movy_mock()?;
        tracing::debug!(
            "test modules are {}",
            compiled_result
                .test_modules()
                .iter()
                .map(|v| v.self_id().name().to_string())
                .join(", ")
        );
        let address =
            executor.deploy_contract(epoch, epoch_ms, deployer.into(), gas, compiled_result)?;
        tracing::info!("publishing {} at {}", root_package_name, address);
        for name in package_names.iter() {
            package_name_map.insert(name.clone(), address.into());
            package_name_map.insert(name.to_ascii_lowercase(), address.into());
        }
        let mut address_aliases: BTreeMap<String, String> = BTreeMap::new();
        for (name, package_addr) in package_name_map.iter() {
            address_aliases
                .entry(package_addr.to_canonical_string(true))
                .or_insert_with(|| name.clone());
        }

        // In search of any deploy functions
        let mut abi = self.db.get_package_info(address.into())?.unwrap();

        for md in abi.modules.iter() {
            if md.is_test_only_module()
                && let Some(init) = md.locate_movy_init()
            {
                let mut builder = ProgrammableTransactionBuilder::new();
                let deployer_arg = builder.pure(ObjectID::from(deployer))?;
                let attacker_arg = builder.pure(ObjectID::from(attacker))?;
                builder.programmable_move_call(
                    address,
                    Identifier::from_str(&md.module_id.module_name).unwrap(),
                    Identifier::from_str(&init.name).unwrap(),
                    vec![],
                    vec![deployer_arg, attacker_arg],
                );
                let ptb = builder.finish();
                tracing::info!("Detected a {} at: {}", MOVY_INIT, md.module_id);
                let tracer = if trace_movy_init {
                    Some(TreeTracer::new_with_aliases(address_aliases.clone()))
                } else {
                    None
                };
                let mut results = executor.run_ptb_with_gas(
                    ptb,
                    epoch,
                    epoch_ms,
                    deployer.into(),
                    gas,
                    tracer,
                )?;
                let trace = if let Some(tracer) = std::mem::take(&mut results.tracer) {
                    Some(tracer.take_inner().pprint_to_error())
                } else {
                    None
                };
                if !results.effects.status().is_ok() {
                    if let Some(trace) = trace {
                        tracing::error!("movy_init reverts with:\n{}", trace);
                    }
                    return Err(eyre!("movy_init reverts!").into());
                }
                tracing::trace!(
                    "movy_init trace:\n{}",
                    trace.unwrap_or_else(|| "-".to_string())
                );
                tracing::info!("Commiting movy_init effects...");
                tracing::debug!(
                    "Status: {:?} Changed Objects: {}, Removed Objects: {}",
                    results.effects.status(),
                    results
                        .effects
                        .all_changed_objects()
                        .iter()
                        .map(|t| format!("{:?}", t))
                        .join(","),
                    results
                        .effects
                        .all_removed_objects()
                        .iter()
                        .map(|t| format!("{:?}", t.0))
                        .join(",")
                );
                self.db
                    .commit_store(results.results.store, &results.results.effects)?;
            }
        }
        non_test_abi.published_at(address.into());
        abi.published_at(address.into());
        Ok((
            address.into(),
            abi,
            non_test_abi,
            package_names,
            package_name_map,
        ))
    }

    pub async fn export_abi(&self) -> Result<BTreeMap<MoveAddress, MovePackageAbi>, MovyError> {
        let objects = self.db.list_objects().await?;

        let mut out = BTreeMap::new();
        for obj in objects {
            if let Ok(Some(abi)) = self.db.get_package_info(obj) {
                // object is package
                out.insert(abi.package_id, abi);
            }
        }
        Ok(out)
    }

    pub async fn load_history(
        &self,
        package_id: MoveAddress,
        ckpt: u64,
        rpc: &GraphQlClient,
    ) -> Result<(), MovyError> {
        if let Some(package) = self.db.get_package_info(package_id)? {
            for module in &package.modules {
                for s in &module.structs {
                    let tag = s.module_id.to_canonical_string(true);
                    let objects = rpc
                        .filter_objects(ckpt, Some(OwnerKind::Shared), None, Some(tag))
                        .await?;
                    for object in objects.into_iter() {
                        self.db.commit_single_object(object)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn load_inner_types(&self) -> Result<(), MovyError> {
        // Analyze all object types in the store
        let objects = self.db.list_objects().await?;
        for obj in objects {
            if let Ok(mv) = self.db.get_move_object_info(obj) {
                let addresses = mv.ty.flat_addresses();
                for addr in addresses {
                    self.db.load_object(addr).await?
                }
            }
        }
        Ok(())
    }

    pub async fn deploy_address(&self, package_id: MoveAddress) -> Result<(), MovyError> {
        let Some(package_object) = self.db.get_object(&package_id.into()) else {
            return Err(eyre!("Package object not found: {}", package_id).into());
        };

        // Analyze package dependencies
        let pkg = package_object
            .data
            .try_as_package()
            .ok_or_else(|| eyre!("Expected package data"))?;
        for upgrade_info in pkg.linkage_table().values() {
            self.db.load_object(upgrade_info.upgraded_id.into()).await?;
        }

        Ok(())
    }

    pub async fn all_tys(&self) -> Result<BTreeSet<MoveStructTag>, MovyError> {
        let mut tags = BTreeSet::new();
        for obj in self.db.list_objects().await? {
            if let Ok(info) = self.db.get_move_object_info(obj) {
                for st in info.ty.flat_structs() {
                    tags.insert(st);
                }
            }
        }
        Ok(tags)
    }
}
