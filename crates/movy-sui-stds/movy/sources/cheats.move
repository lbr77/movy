module movy::cheats;

use sui::tx_context::TxContext;

const TX_HASH_LENGTH: u64 = 32;

/// A cheat scenario for mocking a multi-transaction Sui execution
public struct CheatScenario {
    txn_number: u64,
    ctx: TxContext,
}

/// Begin a new multi-transaction test scenario in a context where `sender` is the tx sender
public fun begin(sender: address): CheatScenario {
    CheatScenario {
        txn_number: 0,
        ctx: new_tx_ctx_from_hint(sender, 0, 0, 0, 0),
    }
}

/// Ends the test scenario
/// Returns the results from the final transaction
/// Will abort if shared or immutable objects were deleted, transferred, or wrapped.
/// Will abort if TransactionEffects cannot be generated
public fun end(scenario: CheatScenario) {
    let CheatScenario { txn_number: _, ctx: _ } = scenario;
    end_transaction()
}


/// Advance the scenario to a new transaction where `sender` is the transaction sender
/// All objects transferred will be moved into the inventories of the account or the global
/// inventory. In other words, in order to access an object with one of the various "take"
/// functions below, e.g. `take_from_address_by_id`, the transaction must first be ended via
/// `next_tx` or `next_with_context`.
/// Returns the results from the previous transaction
/// Will abort if shared or immutable objects were deleted, transferred, or wrapped.
/// Will abort if TransactionEffects cannot be generated
public fun next_tx(scenario: &mut CheatScenario, sender: address) {
    // create a seed for new transaction digest to ensure that this tx has a different
    // digest (and consequently, different object ID's) than the previous tx
    scenario.txn_number = scenario.txn_number + 1;
    let epoch = scenario.ctx.epoch();
    let epoch_timestamp_ms = scenario.ctx.epoch_timestamp_ms();
    scenario.ctx =
        new_tx_ctx_from_hint(
            sender,
            scenario.txn_number,
            epoch,
            epoch_timestamp_ms,
            0,
        );
    end_transaction()
}

/// Advance the scenario to a new epoch and end the transaction
/// See `next_tx` for further details
// public fun next_epoch(scenario: &mut CheatScenario, sender: address) {
//     scenario.ctx.increment_epoch_number();
//     next_tx(scenario, sender)
// }

// /// Advance the scenario to a new epoch, `delta_ms` milliseconds in the future and end
// /// the transaction.
// /// See `next_tx` for further details
// public fun later_epoch(
//     scenario: &mut CheatScenario,
//     delta_ms: u64,
//     sender: address,
// ) {
//     scenario.ctx.increment_epoch_timestamp(delta_ms);
//     next_epoch(scenario, sender)
// }

// /// Advance the scenario to a future `epoch`. Will abort if the `epoch` is in the past.
// public fun skip_to_epoch(scenario: &mut CheatScenario, epoch: u64) {
//     assert!(epoch >= scenario.ctx.epoch());
//     (epoch - scenario.ctx.epoch()).do!(|_| {
//         scenario.ctx.increment_epoch_number();
//         end_transaction()
//     })
// }

public fun dummy_tx_hash_with_hint(hint: u64): vector<u8> {
    let mut tx_hash = std::bcs::to_bytes(&hint);
    while (tx_hash.length() < TX_HASH_LENGTH) tx_hash.push_back(0);
    tx_hash
}

public fun new_tx_ctx_from_hint(
    addr: address,
    hint: u64,
    epoch: u64,
    epoch_timestamp_ms: u64,
    ids_created: u64,
): TxContext {
    new_tx_context(addr, dummy_tx_hash_with_hint(hint), epoch, epoch_timestamp_ms, ids_created)
}

native fun new_tx_context(
    sender: address,
    tx_hash: vector<u8>,
    epoch: u64,
    epoch_timestamp_ms: u64,
    ids_created: u64
): TxContext;

// native fun objects_by_type<T: key>(): vector<ID>;

native fun take_by_id<T: key>(id: ID): T;

// Forward to sui std native call
// native fun share_object_impl<T: key>(obj: T);

// native fun freeze_object_impl<T: key>(obj: T);

native fun end_transaction();

// public fun return_shared<T: key>(t: T) {
//     share_object_impl(t)
// }

// public fun return_immutable<T: key>(t: T) {
//     freeze_object_impl(t)
// }