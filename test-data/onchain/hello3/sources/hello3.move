/*
/// Module: hello3
module hello3::hello3;
*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions


module hello3::hello3;
use hello2::hello2;
use hello4::hello4;

public fun movy_hello3(): u64 {
    hello2::movy_hello2() + hello4::movy_hello4()
}