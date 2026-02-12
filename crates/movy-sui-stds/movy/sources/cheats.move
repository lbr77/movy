module movy::cheats;

use sui::tx_context::TxContext;
use sui::vec_map::VecMap;

const TX_HASH_LENGTH: u64 = 32;


/// Attempted to return an object to the inventory that was not previously removed from the
/// inventory during the current transaction. Can happen if the user attempts to call
/// `return_to_address` on a locally constructed object rather than one returned from a
/// `test_scenario` function such as `take_from_address`.
const ECantReturnObject: u64 = 2;

/// Attempted to retrieve an object of a particular type from the inventory, but it is empty.
/// Can happen if the user already transferred the object or a previous transaction failed to
/// transfer the object to the user.
const EEmptyInventory: u64 = 3;

/// The effects of a transaction
public struct TransactionEffects has drop {
    /// The objects created this transaction
    created: vector<ID>,
    /// The objects written/modified this transaction
    written: vector<ID>,
    /// The objects deleted this transaction
    deleted: vector<ID>,
    /// The objects transferred to an account this transaction
    transferred_to_account: VecMap<ID, /* owner */ address>,
    /// The objects transferred to an object this transaction
    transferred_to_object: VecMap<ID, /* owner */ ID>,
    /// The objects shared this transaction
    shared: vector<ID>,
    /// The objects frozen this transaction
    frozen: vector<ID>,
    /// The number of user events emitted this transaction
    num_user_events: u64,
}

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

public fun ctx(scenario: &mut CheatScenario): &mut TxContext {
    &mut scenario.ctx
}

/// Ends the test scenario
/// Returns the results from the final transaction
/// Will abort if shared or immutable objects were deleted, transferred, or wrapped.
/// Will abort if TransactionEffects cannot be generated
public fun end(scenario: CheatScenario): TransactionEffects {
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
public fun next_tx(scenario: &mut CheatScenario, sender: address): TransactionEffects {
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

/// Helper combining `take_shared_by_id` and `most_recent_id_shared`
/// Aborts if there is no shared object of type `T` in the global inventory
public fun take_shared<T: key>(): T {
    let id_opt = most_recent_id_shared<T>();
    assert!(id_opt.is_some(), EEmptyInventory);
    take_by_id(id_opt.destroy_some())
}

/// Return `t` to the global inventory
public fun return_shared<T: key>(t: T) {
    let id = object::id(&t);
    assert!(was_taken_shared(id), ECantReturnObject);
    share_object_impl(t)
}


native fun was_taken_shared(id: ID): bool;
native fun most_recent_id_shared<T: key>(): Option<ID>;

// native fun objects_by_type<T: key>(): vector<ID>;

native fun take_by_id<T: key>(id: ID): T;

// Forward to sui std native call
native fun share_object_impl<T: key>(obj: T);
// native fun freeze_object_impl<T: key>(obj: T);

native fun end_transaction(): TransactionEffects;

// public fun return_shared<T: key>(t: T) {
//     share_object_impl(t)
// }

// public fun return_immutable<T: key>(t: T) {
//     freeze_object_impl(t)
// }