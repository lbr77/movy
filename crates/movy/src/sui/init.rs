use std::str::FromStr;

use clap::Args;
use color_eyre::eyre::eyre;
use movy_fuzz::utils::{SuperRand, random_seed};
use movy_replay::{db::ObjectStoreMintObject, env::SuiTestingEnv};
use movy_sui::{
    database::{cache::CachedStore, graphql::GraphQlDatabase},
    rpc::{graphql::GraphQlClient, grpc::SuiGrpcArg},
};
use movy_types::{
    error::MovyError,
    input::{MoveAddress, MoveTypeTag},
    object::MoveOwner,
};
use sui_types::base_types::ObjectID;

use crate::sui::{env::SuiTargetArgs, utils::SuiOnchainArguments};

#[derive(Args)]
pub struct SuiInitArgs {
    #[arg(
        short,
        long,
        help = "deployer to use",
        default_value = "0xb64151ee0dd0f7bab72df320c5f8e0c4b784958e7411a6c37d352fe9e176092f"
    )]
    pub deployer: MoveAddress,
    #[arg(
        short,
        long,
        help = "attacker to use",
        default_value = "0xa773c4c5ef0b74150638fcfe8b0cd1bb3bbf6f1af963715168ad909bbaf2eddb"
    )]
    pub attacker: MoveAddress,
    #[arg(
        short,
        long,
        help = "rpc to use",
        default_value = "https://fullnode.mainnet.sui.io"
    )]
    pub rpc: SuiGrpcArg,
    #[clap(flatten)]
    pub onchain: SuiOnchainArguments,
    #[clap(flatten)]
    pub target: SuiTargetArgs,
}

impl SuiInitArgs {
    pub async fn run(self) -> Result<(), MovyError> {
        if self.target.locals.as_ref().is_none_or(|v| v.is_empty()) {
            return Err(eyre!("Please pass at least one local package via --locals").into());
        }

        let mut rand = SuperRand::new(random_seed());
        let graphql = GraphQlClient::new_mystens();
        let _rpc = self.rpc.grpc().await?;
        let primitives = self
            .onchain
            .resolve_onchain_primitives(Some(&graphql))
            .await?;

        let env = CachedStore::new(GraphQlDatabase::new_client(
            graphql.clone(),
            primitives.checkpoint,
        ));
        let gas_id = ObjectID::random_from_rng(&mut rand);
        env.mint_coin_id(
            MoveTypeTag::from_str("0x2::sui::SUI").unwrap(),
            MoveOwner::AddressOwner(self.deployer),
            gas_id.into(),
            100_000_000_000,
        )?;

        let testing_env = SuiTestingEnv::new(env.wrapped());
        testing_env.mock_testing_std()?;
        testing_env.install_movy()?;

        let (target_packages, _, _) = self
            .target
            .build_env(
                &testing_env,
                primitives.checkpoint,
                primitives.epoch,
                primitives.epoch_ms,
                self.deployer,
                self.attacker,
                gas_id.into(),
                &graphql,
            )
            .await?;

        println!(
            "movy_init finished for {} package(s) at checkpoint {}.",
            target_packages.len(),
            primitives.checkpoint
        );

        Ok(())
    }
}
