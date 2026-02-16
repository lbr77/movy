use move_binary_format::file_format::Bytecode;
use move_trace_format::{
    format::{Effect, Frame, TraceEvent, TraceIndex, TraceValue},
    interface::Tracer,
};
use sui_types::TypeTag;

use crate::tracer::state::TraceState;

pub mod concolic;
pub mod extra;
pub mod fuzz;
pub mod op;
pub mod oracle;
pub mod state;
pub mod tree;

#[auto_impl::auto_impl(&mut)]
pub trait MovySuiTracerExt {
    // return `false` will skip the event
    fn on_raw_event(&mut self, _state: &TraceState, _ev: &TraceEvent) -> bool {
        false
    }
    fn on_move_call(&mut self, _state: &TraceState) {}
    fn on_effect(&mut self, _state: &TraceState, _effect: &Box<Effect>) {}
    fn open_frame(&mut self, _state: &TraceState, _frame: &Box<Frame>, _gas_left: u64) {}
    fn close_frame(
        &mut self,
        _state: &TraceState,
        _frame_id: TraceIndex,
        _return_: &Vec<TraceValue>,
        _gas_left: u64,
    ) {
    }
    fn before_instruction(
        &mut self,
        _state: &TraceState,
        _tys: &Vec<TypeTag>,
        _pc: u16,
        _gas_left: u64,
        _instruction: &Bytecode,
    ) {
    }
}

pub struct MovySuiTracerWrapper<T> {
    pub tracer: T,
    pub state: TraceState,
}

impl<T: MovySuiTracerExt> From<T> for MovySuiTracerWrapper<T> {
    fn from(value: T) -> Self {
        Self {
            tracer: value,
            state: TraceState::new(),
        }
    }
}

impl<T: MovySuiTracerExt> Tracer for MovySuiTracerWrapper<T> {
    fn notify(
        &mut self,
        event: &TraceEvent,
        _writer: &mut move_trace_format::interface::Writer<'_>,
        _stack: Option<&move_vm_stack::Stack>,
    ) {
        if !self.tracer.on_raw_event(&self.state, event) {
            self.state.apply_event(event);
            return;
        }
        match event {
            TraceEvent::External(e) => {
                if e.as_str().is_some_and(|s| s == "MoveCallStart") {
                    self.tracer.on_move_call(&self.state);
                }
                self.state.apply_event(event);
            }
            TraceEvent::OpenFrame { frame, gas_left } => {
                self.tracer.open_frame(&self.state, frame, *gas_left);
                self.state.apply_event(event);
            }
            TraceEvent::CloseFrame {
                frame_id,
                return_,
                gas_left,
            } => {
                self.tracer
                    .close_frame(&self.state, *frame_id, return_, *gas_left);
                self.state.apply_event(event);
            }
            TraceEvent::Effect(e) => {
                self.tracer.on_effect(&self.state, e);
                self.state.apply_event(event);
            }
            TraceEvent::Instruction {
                type_parameters,
                pc,
                gas_left,
                instruction,
            } => {
                self.tracer.before_instruction(
                    &self.state,
                    type_parameters,
                    *pc,
                    *gas_left,
                    instruction,
                );
                self.state.apply_event(event);
            }
            _ => {
                self.state.apply_event(event);
            }
        }
    }
}

#[derive(Default)]
pub struct NopTracer;

impl Tracer for NopTracer {
    fn notify(
        &mut self,
        _event: &TraceEvent,
        _writer: &mut move_trace_format::interface::Writer<'_>,
        _stack: Option<&move_vm_stack::Stack>,
    ) {
    }
}

impl MovySuiTracerExt for NopTracer {}

pub enum SelectiveTracer<T1, T2> {
    T1(T1),
    T2(T2),
}

impl<T1, T2> Tracer for SelectiveTracer<T1, T2>
where
    T1: Tracer,
    T2: Tracer,
{
    fn notify(
        &mut self,
        event: &TraceEvent,
        writer: &mut move_trace_format::interface::Writer<'_>,
        stack: Option<&move_vm_stack::Stack>,
    ) {
        match self {
            Self::T1(t) => t.notify(event, writer, stack),
            Self::T2(t) => t.notify(event, writer, stack),
        }
    }
}

impl<T1, T2> MovySuiTracerExt for SelectiveTracer<T1, T2>
where
    T1: MovySuiTracerExt,
    T2: MovySuiTracerExt,
{
    fn on_raw_event(&mut self, state: &TraceState, ev: &TraceEvent) -> bool {
        match self {
            Self::T1(v) => v.on_raw_event(state, ev),
            Self::T2(v) => v.on_raw_event(state, ev),
        }
    }
    fn on_move_call(&mut self, state: &TraceState) {
        match self {
            Self::T1(v) => v.on_move_call(state),
            Self::T2(v) => v.on_move_call(state),
        }
    }
    fn on_effect(&mut self, state: &TraceState, effect: &Box<Effect>) {
        match self {
            Self::T1(v) => v.on_effect(state, effect),
            Self::T2(v) => v.on_effect(state, effect),
        }
    }
    fn before_instruction(
        &mut self,
        state: &TraceState,
        tys: &Vec<TypeTag>,
        pc: u16,
        gas_left: u64,
        instruction: &Bytecode,
    ) {
        match self {
            Self::T1(v) => v.before_instruction(state, tys, pc, gas_left, instruction),
            Self::T2(v) => v.before_instruction(state, tys, pc, gas_left, instruction),
        }
    }

    fn open_frame(&mut self, state: &TraceState, frame: &Box<Frame>, gas_left: u64) {
        match self {
            Self::T1(v) => v.open_frame(state, frame, gas_left),
            Self::T2(v) => v.open_frame(state, frame, gas_left),
        }
    }

    fn close_frame(
        &mut self,
        state: &TraceState,
        frame_id: TraceIndex,
        return_: &Vec<TraceValue>,
        gas_left: u64,
    ) {
        match self {
            Self::T1(v) => v.close_frame(state, frame_id, return_, gas_left),
            Self::T2(v) => v.close_frame(state, frame_id, return_, gas_left),
        }
    }
}

#[derive(Default)]
pub struct MayEnableTracer<T> {
    pub tracer: Option<T>,
}

impl<T> MayEnableTracer<T> {
    pub fn new(tracer: T) -> Self {
        Self {
            tracer: Some(tracer),
        }
    }
}

impl<T> Tracer for MayEnableTracer<T>
where
    T: Tracer,
{
    fn notify(
        &mut self,
        event: &TraceEvent,
        writer: &mut move_trace_format::interface::Writer<'_>,
        stack: Option<&move_vm_stack::Stack>,
    ) {
        if let Some(tracer) = &mut self.tracer {
            tracer.notify(event, writer, stack);
        }
    }
}

pub struct CombinedTracer<T1, T2> {
    pub t1: T1,
    pub t2: T2,
}

impl<T1, T2> Tracer for CombinedTracer<T1, T2>
where
    T1: Tracer,
    T2: Tracer,
{
    fn notify(
        &mut self,
        event: &TraceEvent,
        writer: &mut move_trace_format::interface::Writer<'_>,
        stack: Option<&move_vm_stack::Stack>,
    ) {
        self.t1.notify(event, writer, stack);
        self.t2.notify(event, writer, stack);
    }
}

impl<T1, T2> MovySuiTracerExt for CombinedTracer<T1, T2>
where
    T1: MovySuiTracerExt,
    T2: MovySuiTracerExt,
{
    fn on_raw_event(&mut self, state: &TraceState, ev: &TraceEvent) -> bool {
        self.t1.on_raw_event(state, ev) && self.t2.on_raw_event(state, ev)
    }
    fn on_move_call(&mut self, state: &TraceState) {
        self.t1.on_move_call(state);
        self.t2.on_move_call(state);
    }
    fn on_effect(&mut self, state: &TraceState, effect: &Box<Effect>) {
        self.t1.on_effect(state, effect);
        self.t2.on_effect(state, effect);
    }
    fn before_instruction(
        &mut self,
        state: &TraceState,
        tys: &Vec<TypeTag>,
        pc: u16,
        gas_left: u64,
        instruction: &Bytecode,
    ) {
        self.t1
            .before_instruction(state, tys, pc, gas_left, instruction);
        self.t2
            .before_instruction(state, tys, pc, gas_left, instruction);
    }

    fn close_frame(
        &mut self,
        state: &TraceState,
        frame_id: TraceIndex,
        return_: &Vec<TraceValue>,
        gas_left: u64,
    ) {
        self.t1.close_frame(state, frame_id, return_, gas_left);
        self.t2.close_frame(state, frame_id, return_, gas_left);
    }

    fn open_frame(&mut self, state: &TraceState, frame: &Box<Frame>, gas_left: u64) {
        self.t1.open_frame(state, frame, gas_left);
        self.t2.open_frame(state, frame, gas_left);
    }
}
