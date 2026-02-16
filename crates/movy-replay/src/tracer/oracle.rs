use move_binary_format::file_format::Bytecode;
use move_trace_format::format::Frame;
use movy_types::{
    error::MovyError,
    input::{FunctionIdent, MoveSequence},
    oracle::OracleFinding,
};
use sui_types::{effects::TransactionEffects, storage::ObjectStore};

use crate::tracer::{concolic::ConcolicState, state::TraceState};

pub trait SuiGeneralOracle<S> {
    fn pre_execution<T: ObjectStore>(
        &mut self,
        _db: T,
        _state: &mut S,
        _sequence: &MoveSequence,
    ) -> Result<(), MovyError> {
        Ok(())
    }

    fn open_frame(
        &mut self,
        _frame: &Box<Frame>,
        _trace_state: &TraceState,
        _symbol_stack: &ConcolicState,
        _current_function: &FunctionIdent,
        _state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        Ok(vec![])
    }

    fn before_instruction(
        &mut self,
        _pc: u16,
        _bytecode: &Bytecode,
        _trace_state: &TraceState,
        _symbol_stack: &ConcolicState,
        _current_function: &FunctionIdent,
        _state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        Ok(vec![])
    }

    fn done_execution<T: ObjectStore>(
        &mut self,
        _db: T,
        _state: &mut S,
        _effects: &TransactionEffects,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        Ok(vec![])
    }
}

impl<S> SuiGeneralOracle<S> for () {
    fn pre_execution<T>(
        &mut self,
        _db: T,
        _state: &mut S,
        _sequence: &MoveSequence,
    ) -> Result<(), MovyError> {
        Ok(())
    }

    fn before_instruction(
        &mut self,
        _pc: u16,
        _bytecode: &Bytecode,
        _trace_state: &TraceState,
        _symbol_stack: &ConcolicState,
        _current_function: &FunctionIdent,
        _state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        Ok(vec![])
    }

    fn done_execution<T>(
        &mut self,
        _db: T,
        _state: &mut S,
        _effects: &TransactionEffects,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        Ok(vec![])
    }
}

impl<S, O1, O2> SuiGeneralOracle<S> for (O1, O2)
where
    O1: SuiGeneralOracle<S>,
    O2: SuiGeneralOracle<S>,
{
    fn pre_execution<T: ObjectStore>(
        &mut self,
        db: T,
        state: &mut S,
        sequence: &MoveSequence,
    ) -> Result<(), MovyError> {
        self.0.pre_execution(&db, state, sequence)?;
        self.1.pre_execution(&db, state, sequence)
    }

    fn open_frame(
        &mut self,
        frame: &Box<Frame>,
        trace_state: &TraceState,
        symbol_stack: &ConcolicState,
        current_function: &FunctionIdent,
        state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        Ok(self
            .0
            .open_frame(frame, trace_state, symbol_stack, current_function, state)?
            .into_iter()
            .chain(
                self.1
                    .open_frame(frame, trace_state, symbol_stack, current_function, state)?,
            )
            .collect())
    }

    fn before_instruction(
        &mut self,
        pc: u16,
        bytecode: &Bytecode,
        trace_state: &TraceState,
        symbol_stack: &ConcolicState,
        current_function: &FunctionIdent,
        state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        Ok(self
            .0
            .before_instruction(
                pc,
                bytecode,
                trace_state,
                symbol_stack,
                current_function,
                state,
            )?
            .into_iter()
            .chain(self.1.before_instruction(
                pc,
                bytecode,
                trace_state,
                symbol_stack,
                current_function,
                state,
            )?)
            .collect())
    }

    fn done_execution<T: ObjectStore>(
        &mut self,
        db: T,
        state: &mut S,
        effects: &TransactionEffects,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        Ok(self
            .0
            .done_execution(&db, state, effects)?
            .into_iter()
            .chain(self.1.done_execution(&db, state, effects)?)
            .collect())
    }
}

pub struct CouldDisabledOralce<O> {
    pub oracle: O,
    pub disabled: bool,
}

impl<O> CouldDisabledOralce<O> {
    pub fn new(oracle: O, disabled: bool) -> Self {
        Self { oracle, disabled }
    }
}

impl<O, S> SuiGeneralOracle<S> for CouldDisabledOralce<O>
where
    O: SuiGeneralOracle<S>,
{
    fn pre_execution<T: ObjectStore>(
        &mut self,
        db: T,
        state: &mut S,
        sequence: &MoveSequence,
    ) -> Result<(), MovyError> {
        if self.disabled {
            return Ok(());
        }
        self.oracle.pre_execution(db, state, sequence)
    }

    fn open_frame(
        &mut self,
        frame: &Box<Frame>,
        trace_state: &TraceState,
        symbol_stack: &ConcolicState,
        current_function: &FunctionIdent,
        state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        if self.disabled {
            return Ok(vec![]);
        }

        self.oracle
            .open_frame(frame, trace_state, symbol_stack, current_function, state)
    }

    fn before_instruction(
        &mut self,
        pc: u16,
        bytecode: &Bytecode,
        trace_state: &TraceState,
        symbol_stack: &ConcolicState,
        current_function: &FunctionIdent,
        state: &mut S,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        if self.disabled {
            return Ok(vec![]);
        }
        self.oracle.before_instruction(
            pc,
            bytecode,
            trace_state,
            symbol_stack,
            current_function,
            state,
        )
    }

    fn done_execution<T: ObjectStore>(
        &mut self,
        db: T,
        state: &mut S,
        effects: &TransactionEffects,
    ) -> Result<Vec<OracleFinding>, MovyError> {
        if self.disabled {
            return Ok(vec![]);
        }
        self.oracle.done_execution(db, state, effects)
    }
}
