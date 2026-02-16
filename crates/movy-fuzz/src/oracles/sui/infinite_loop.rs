use std::collections::BTreeMap;

use move_binary_format::file_format::Bytecode;
use serde_json::json;

use movy_replay::tracer::{
    concolic::{ConcolicState, SymbolValue},
    oracle::SuiGeneralOracle,
};
use movy_types::{
    error::MovyError,
    oracle::{OracleFinding, Severity},
};

use crate::utils::hash_to_u64;

#[derive(Debug, Default, Clone)]
pub struct InfiniteLoopOracle {
    pub branch_counts: BTreeMap<u64, BTreeMap<u16, (u64, usize)>>,
}

impl<S> SuiGeneralOracle<S> for InfiniteLoopOracle {
    fn open_frame(
        &mut self,
        frame: &Box<move_trace_format::format::Frame>,
        _trace_state: &movy_replay::tracer::state::TraceState,
        _symbol_stack: &ConcolicState,
        _current_function: &movy_types::input::FunctionIdent,
        _state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        let key = format!("{}::{}", frame.module, frame.function_name);
        let key = hash_to_u64(&key);
        self.branch_counts.remove(&key);
        Ok(vec![])
    }

    fn before_instruction(
        &mut self,
        pc: u16,
        instruction: &Bytecode,
        trace_state: &movy_replay::tracer::state::TraceState,
        symbol_stack: &ConcolicState,
        current_function: &movy_types::input::FunctionIdent,
        state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        match instruction {
            Bytecode::BrFalse(_) | Bytecode::BrTrue(_) => {
                let func = current_function;
                if symbol_stack.stack.is_empty() {
                    return Ok(vec![]);
                }
                let cond_symbol = &symbol_stack.stack[symbol_stack.stack.len() - 1];
                match cond_symbol {
                    SymbolValue::Unknown => return Ok(vec![]),
                    SymbolValue::Value(v) => {
                        let key = format!("{}::{}", func.0, func.1);
                        let key = hash_to_u64(&key);
                        let v = hash_to_u64(&v.to_string());
                        let count = self
                            .branch_counts
                            .entry(key)
                            .or_default()
                            .entry(pc)
                            .or_default();
                        if count.0 != v {
                            count.0 = v;
                            count.1 = 1;
                        } else {
                            if count.1 >= 1000 {
                                count.1 = 0;
                                let info = json!({
                                    "oracle": "InfiniteLoopOracle",
                                    "function": current_function.to_string(),
                                    "pc": pc,
                                });
                                return Ok(vec![OracleFinding {
                                    oracle: "InfiniteLoopOracle".to_string(),
                                    severity: Severity::Major,
                                    extra: info,
                                }]);
                            }
                            count.1 += 1;
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(vec![])
    }
}
