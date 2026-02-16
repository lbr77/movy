use itertools::Itertools;
use move_trace_format::{
    format::{DataLoad, Effect, Location, Read, TraceEvent, TraceIndex, TraceValue, Write},
    value::SerializableMoveValue,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct TraceState {
    // Tracks "global memory" state (i.e., references out in to global memory/references returned
    // from native functions).
    pub loaded_state: BTreeMap<TraceIndex, SerializableMoveValue>,
    // The current state (i.e., values) of the VM's operand stack.
    pub operand_stack: Vec<TraceValue>,
    // The current call stack indexed by frame id. Maps from the frame id to the current state of
    // the frame's locals. The bool indicates if the frame is native or not.
    pub call_stack: BTreeMap<TraceIndex, (BTreeMap<usize, TraceValue>, bool)>,
}

pub fn format_trace_value(val: &TraceValue) -> String {
    match val {
        TraceValue::RuntimeValue { value } => {
            format!("TraceValue({})", format_serializable_move_value(value))
        }
        TraceValue::ImmRef {
            location: _,
            snapshot,
        } => {
            format!("TraceValue(&{})", format_serializable_move_value(snapshot))
        }
        TraceValue::MutRef {
            location: _,
            snapshot,
        } => {
            format!(
                "TraceValue(&mut {})",
                format_serializable_move_value(snapshot)
            )
        }
    }
}

pub fn format_serializable_move_value(val: &SerializableMoveValue) -> String {
    match val {
        SerializableMoveValue::Address(a) => format!("Address({})", a.to_canonical_string(true)),
        SerializableMoveValue::Bool(b) => format!("Bool({})", b),
        SerializableMoveValue::U8(v) => format!("U8({})", v),
        SerializableMoveValue::U16(v) => format!("U16({})", v),
        SerializableMoveValue::U32(v) => format!("U32({})", v),
        SerializableMoveValue::U64(v) => format!("U64({})", v),
        SerializableMoveValue::U128(v) => format!("U128({})", v),
        SerializableMoveValue::U256(v) => format!("U256({})", v),
        SerializableMoveValue::Struct(s) => {
            let fields: Vec<String> = s
                .fields
                .iter()
                .map(|(id, v)| format!("{}: {}", id, format_serializable_move_value(v)))
                .collect();
            format!("{} {{ {} }}", s.type_, fields.join(", "))
        }
        SerializableMoveValue::Vector(elems) => {
            // Specialize vector<u8> as a hex string.
            let all_u8: Option<Vec<u8>> = elems
                .iter()
                .map(|e| match e {
                    SerializableMoveValue::U8(b) => Some(*b),
                    _ => None,
                })
                .collect();
            if let Some(bytes) = all_u8 {
                format!("Vector<u8>[0x{}]", const_hex::encode(&bytes))
            } else {
                let items: Vec<String> = elems.iter().map(format_serializable_move_value).collect();
                format!("Vector[{}]", items.join(", "))
            }
        }
        SerializableMoveValue::Variant(v) => {
            let fields: Vec<String> = v
                .fields
                .iter()
                .map(|(id, val)| format!("{}: {}", id, format_serializable_move_value(val)))
                .collect();
            format!("{}::{}({})", v.type_, v.variant_name, fields.join(", "))
        }
    }
}

impl TraceState {
    pub fn new() -> Self {
        Self {
            loaded_state: BTreeMap::new(),
            operand_stack: vec![],
            call_stack: BTreeMap::new(),
        }
    }

    pub fn operand_stack_debug(&self) -> String {
        format!(
            "OperandStack([{}])",
            self.operand_stack.iter().map(format_trace_value).join(", ")
        )
    }

    /// Apply an event to the state machine and update the locals state accordingly.
    pub fn apply_event(&mut self, event: &TraceEvent) {
        match event {
            TraceEvent::OpenFrame { frame, .. } => {
                let mut locals = BTreeMap::new();
                for (i, p) in frame.parameters.iter().enumerate() {
                    // NB: parameters are passed directly, so we just pop to make sure they aren't also
                    // left on the operand stack. For the initial call, these pops may (should) fail, but that
                    // is fine as we already have the values in the parameter list.
                    self.operand_stack.pop();
                    locals.insert(i, p.clone());
                }

                self.call_stack
                    .insert(frame.frame_id, (locals, frame.is_native));
            }
            TraceEvent::CloseFrame { .. } => {
                self.call_stack
                    .pop_last()
                    .expect("Unbalanced call stack in memory tracer -- this should never happen");
            }
            TraceEvent::Effect(ef) => match &**ef {
                Effect::ExecutionError(_) => (),
                Effect::Push(value) => {
                    self.operand_stack.push(value.clone());
                }
                Effect::Pop(_) => {
                    self.operand_stack.pop().expect(
                        "Tried to pop off the empty operand stack -- this should never happen",
                    );
                }
                Effect::Read(Read {
                    location,
                    root_value_read: _,
                    moved,
                }) => {
                    if *moved {
                        match location {
                            Location::Local(frame_idx, idx) => {
                                let frame = self.call_stack.get_mut(frame_idx).unwrap();
                                frame.0.remove(idx);
                            }
                            Location::Indexed(..) => {
                                panic!("Cannot move from indexed location");
                            }
                            Location::Global(..) => {
                                panic!("Cannot move from global location");
                            }
                        }
                    }
                }
                Effect::Write(Write {
                    location,
                    root_value_after_write: value_written,
                }) => match location {
                    Location::Local(frame_idx, idx) => {
                        let frame = self.call_stack.get_mut(frame_idx).unwrap();
                        frame.0.insert(*idx, value_written.clone());
                    }
                    Location::Indexed(location, _idx) => {
                        let val = self.get_mut_location(location);
                        *val = value_written.clone().snapshot().clone();
                    }
                    Location::Global(id) => {
                        let val = self.loaded_state.get_mut(id).unwrap();
                        *val = value_written.snapshot().clone();
                    }
                },
                Effect::DataLoad(DataLoad {
                    location, snapshot, ..
                }) => {
                    let Location::Global(id) = location else {
                        unreachable!("Dataload by reference must have a global location");
                    };
                    self.loaded_state.insert(*id, snapshot.clone());
                }
            },
            // External events are treated opaquely unless they define a trace boundary.
            TraceEvent::External(external) => {
                if external.as_str().is_some_and(|s| s == "MoveCallStart") {
                    // New trace boundary. Reset reconstructed runtime state to avoid
                    // leaking values across calls/txns.
                    self.loaded_state.clear();
                    self.operand_stack.clear();
                    self.call_stack.clear();
                }
            }
            // Instructions
            _ => (),
        }
    }

    /// Given a reference "location" return a mutable reference to the value it points to so that
    /// it can be updated.
    fn get_mut_location(&mut self, location: &Location) -> &mut SerializableMoveValue {
        match location {
            Location::Local(frame_idx, idx) => {
                let frame = self.call_stack.get_mut(frame_idx).unwrap();
                frame.0.get_mut(idx).unwrap().value_mut().unwrap()
            }
            Location::Indexed(loc, _offset) => self.get_mut_location(loc),
            Location::Global(id) => self.loaded_state.get_mut(id).unwrap(),
        }
    }
}

impl Default for TraceState {
    fn default() -> Self {
        Self::new()
    }
}
