module movy::cheats;

use sui::tx_context::TxContext;

const TX_HASH_LENGTH: u64 = 32;

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

public native fun new_tx_context(sender: address, tx_hash: vector<u8>, epoch: u64, epoch_timestamp_ms: u64, ids_created: u64): TxContext;