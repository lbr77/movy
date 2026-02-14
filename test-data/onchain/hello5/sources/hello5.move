/*
/// Module: hello5
module hello5::hello5;
*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions


module hello5::hello5;
use hello1::hello1;

public fun movy_hello5(): u64 {
    hello1::movy_hello1()
}