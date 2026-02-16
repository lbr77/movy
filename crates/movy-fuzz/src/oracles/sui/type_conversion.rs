use move_binary_format::file_format::Bytecode;
use movy_types::oracle::OracleFinding;
use serde_json::json;

use movy_replay::tracer::concolic::value_bitwidth;
use movy_replay::tracer::{concolic::ConcolicState, oracle::SuiGeneralOracle};
use movy_types::error::MovyError;

#[derive(Debug, Default, Clone, Copy)]
pub struct TypeConversionOracle;

impl<S> SuiGeneralOracle<S> for TypeConversionOracle {
    fn before_instruction(
        &mut self,
        pc: u16,
        instruction: &Bytecode,
        trace_state: &movy_replay::tracer::state::TraceState,
        _symbol_stack: &ConcolicState,
        current_function: &movy_types::input::FunctionIdent,
        _state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        let stack = &trace_state.operand_stack;
        if stack.is_empty() {
            return Ok(vec![]);
        }
        let val = &stack[stack.len() - 1];
        let unnecessary = match instruction {
            Bytecode::CastU8 => value_bitwidth(val) == 8,
            Bytecode::CastU16 => value_bitwidth(val) == 16,
            Bytecode::CastU32 => value_bitwidth(val) == 32,
            Bytecode::CastU64 => value_bitwidth(val) == 64,
            Bytecode::CastU128 => value_bitwidth(val) == 128,
            Bytecode::CastU256 => value_bitwidth(val) == 256,
            _ => false,
        };
        if unnecessary {
            let info = json!({
                "oracle": "TypeConversionOracle",
                "function": current_function.to_string(),
                "pc": pc,
            });
            return Ok(vec![OracleFinding {
                oracle: "TypeConversionOracle".to_string(),
                severity: movy_types::oracle::Severity::Minor,
                extra: info,
            }]);
        }
        Ok(vec![])
    }
}
