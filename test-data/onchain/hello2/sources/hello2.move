/*
/// Module: hello2
module hello2::hello2;
*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions


module hello2::hello2;
use hello1::hello1;

public fun movy_hello2(): u64 {
    42 + hello1::movy_hello1()
}