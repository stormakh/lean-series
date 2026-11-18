//! Port of `promql/parser/functions.go`: the functions of PromQL and their types.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use super::functions_table::FUNCTION_DEFS;
use super::value::ValueType;

/// The id of a built-in function: its index in the sorted table of [`functions`]. The engine dispatches on it
/// without hashing names. Functions that are not built in (custom functions, unknown names in a partial AST)
/// have [`FunctionId::CUSTOM`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FunctionId(pub u16);

impl FunctionId {
    /// The id of a function that is not built in.
    pub const CUSTOM: FunctionId = FunctionId(u16::MAX);
}

/// A function of the expression language.
#[derive(Clone, Debug, PartialEq)]
pub struct Function {
    pub name: Cow<'static, str>,
    pub arg_types: Cow<'static, [ValueType]>,
    /// 0: fixed arguments; n > 0: up to n more arguments of the last type; -1: any number more.
    pub variadic: i32,
    pub return_type: ValueType,
    pub experimental: bool,
    /// The built-in function id, or [`FunctionId::CUSTOM`].
    pub id: FunctionId,
}

impl Function {
    /// A function that is not built in.
    pub fn new(
        name: &str,
        arg_types: Vec<ValueType>,
        variadic: i32,
        return_type: ValueType,
        experimental: bool,
    ) -> Self {
        Function {
            name: Cow::Owned(name.to_string()),
            arg_types: Cow::Owned(arg_types),
            variadic,
            return_type,
            experimental,
            id: FunctionId::CUSTOM,
        }
    }
}

/// A row of the generated function table.
pub(crate) struct FunctionDef {
    pub name: &'static str,
    pub arg_types: &'static [ValueType],
    pub variadic: i32,
    pub return_type: ValueType,
    pub experimental: bool,
}

/// A set of functions keyed by name.
pub type FunctionMap = HashMap<String, Arc<Function>>;

/// All functions supported by PromQL (Go `Functions`).
pub fn functions() -> &'static FunctionMap {
    static FUNCS: OnceLock<FunctionMap> = OnceLock::new();
    FUNCS.get_or_init(|| {
        FUNCTION_DEFS
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let f = Function {
                    name: Cow::Borrowed(d.name),
                    arg_types: Cow::Borrowed(d.arg_types),
                    variadic: d.variadic,
                    return_type: d.return_type,
                    experimental: d.experimental,
                    id: FunctionId(i as u16),
                };
                (d.name.to_string(), Arc::new(f))
            })
            .collect()
    })
}

/// The built-in function with the given id.
pub fn function_by_id(id: FunctionId) -> Option<&'static Arc<Function>> {
    let d = FUNCTION_DEFS.get(id.0 as usize)?;
    functions().get(d.name)
}

/// Go `getFunction`.
pub(crate) fn get_function<'f>(name: &str, functions: &'f FunctionMap) -> Option<&'f Arc<Function>> {
    functions.get(name)
}

/// Go `MustGetFunction`: the built-in function with the given name; panics if there is none.
pub fn must_get_function(name: &str) -> Arc<Function> {
    match functions().get(name) {
        Some(f) => f.clone(),
        None => panic!("function {} does not exist", super::quote::go_quote(name)),
    }
}
