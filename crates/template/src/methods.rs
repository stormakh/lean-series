//! Methods of the Go types that templates call: `time.Time`, `time.Duration`, `time.Month`, `time.Weekday` and
//! `*histogram.FloatHistogram` (Go method sets; a pointer has the methods of its element).

use crate::gotime::{self, GoTime};
use crate::value::{Func, IntKind, Type, Value};

/// The `String()` result of a value that implements `fmt.Stringer`.
pub fn stringer(v: &Value) -> Option<String> {
    match v {
        Value::Int(d, IntKind::Duration) => Some(gotime::duration_string(*d)),
        Value::Int(m, IntKind::Month) => Some(gotime::month_string(*m)),
        Value::Int(d, IntKind::Weekday) => Some(gotime::weekday_string(*d)),
        Value::Time(t) => Some(t.string()),
        Value::Histogram(h) => Some(h.string()),
        Value::Iface(inner) => stringer(inner),
        Value::Ptr(inner) => match &**inner {
            Value::Int(_, IntKind::Duration | IntKind::Month | IntKind::Weekday)
            | Value::Time(_)
            | Value::Histogram(_) => stringer(inner),
            _ => None,
        },
        _ => None,
    }
}

fn dur() -> Type {
    Type::Int(IntKind::Duration)
}

fn s() -> Type {
    Type::String(crate::value::StrKind::String)
}

fn int64() -> Type {
    Type::Int(IntKind::Int64)
}

fn int() -> Type {
    Type::Int(IntKind::Int)
}

fn time_arg(v: &Value) -> GoTime {
    match v {
        Value::Time(t) => *t,
        _ => GoTime::ZERO,
    }
}

fn int_arg(v: &Value) -> i64 {
    match v {
        Value::Int(i, _) => *i,
        _ => 0,
    }
}

/// The method `name` of `recv`, bound to the receiver (Go `reflect.Value.MethodByName`).
pub fn method(recv: &Value, name: &str) -> Option<Func> {
    match recv {
        Value::Ptr(inner) | Value::Iface(inner) => method(inner, name),
        Value::Time(t) => time_method(*t, name),
        Value::Int(d, IntKind::Duration) => duration_method(*d, name),
        Value::Int(m, IntKind::Month) if name == "String" => {
            let m = *m;
            Some(Func::new(vec![], s(), false, move |_| Ok(Value::string(gotime::month_string(m)))))
        }
        Value::Int(d, IntKind::Weekday) if name == "String" => {
            let d = *d;
            Some(Func::new(vec![], s(), false, move |_| Ok(Value::string(gotime::weekday_string(d)))))
        }
        Value::Histogram(h) if name == "String" => {
            let h = h.clone();
            Some(Func::new(vec![], s(), false, move |_| Ok(Value::string(h.string()))))
        }
        _ => None,
    }
}

fn time_method(t: GoTime, name: &str) -> Option<Func> {
    let tm = Type::Time;
    Some(match name {
        "String" => Func::new(vec![], s(), false, move |_| Ok(Value::string(t.string()))),
        "Format" => Func::new(vec![s()], s(), false, move |a| {
            Ok(Value::string(t.format(a[0].as_str().unwrap_or(""))))
        }),
        "Unix" => Func::new(vec![], int64(), false, move |_| Ok(Value::Int(t.unix(), IntKind::Int64))),
        "UnixMilli" => Func::new(vec![], int64(), false, move |_| Ok(Value::Int(t.unix_milli(), IntKind::Int64))),
        "UnixMicro" => Func::new(vec![], int64(), false, move |_| Ok(Value::Int(t.unix_micro(), IntKind::Int64))),
        "UnixNano" => Func::new(vec![], int64(), false, move |_| Ok(Value::Int(t.unix_nano(), IntKind::Int64))),
        "Add" => Func::new(vec![dur()], tm, false, move |a| Ok(Value::Time(t.add(int_arg(&a[0]))))),
        "Sub" => Func::new(vec![Type::Time], dur(), false, move |a| Ok(Value::duration(t.sub(&time_arg(&a[0]))))),
        "AddDate" => Func::new(vec![int(), int(), int()], tm, false, move |a| {
            Ok(Value::Time(t.add_date(int_arg(&a[0]), int_arg(&a[1]), int_arg(&a[2]))))
        }),
        "Year" => Func::new(vec![], int(), false, move |_| Ok(Value::int(t.date().0))),
        "Month" => Func::new(vec![], Type::Int(IntKind::Month), false, move |_| Ok(Value::Int(t.date().1, IntKind::Month))),
        "Day" => Func::new(vec![], int(), false, move |_| Ok(Value::int(t.date().2))),
        "Hour" => Func::new(vec![], int(), false, move |_| Ok(Value::int(t.clock().0))),
        "Minute" => Func::new(vec![], int(), false, move |_| Ok(Value::int(t.clock().1))),
        "Second" => Func::new(vec![], int(), false, move |_| Ok(Value::int(t.clock().2))),
        "Nanosecond" => Func::new(vec![], int(), false, move |_| Ok(Value::int(t.nsec as i64))),
        "YearDay" => Func::new(vec![], int(), false, move |_| Ok(Value::int(t.year_day()))),
        "Weekday" => Func::new(vec![], Type::Int(IntKind::Weekday), false, move |_| {
            Ok(Value::Int(t.weekday(), IntKind::Weekday))
        }),
        "UTC" | "Local" => Func::new(vec![], tm, false, move |_| Ok(Value::Time(t))),
        "Before" => Func::new(vec![Type::Time], Type::Bool, false, move |a| Ok(Value::Bool(t < time_arg(&a[0])))),
        "After" => Func::new(vec![Type::Time], Type::Bool, false, move |a| Ok(Value::Bool(t > time_arg(&a[0])))),
        "Equal" => Func::new(vec![Type::Time], Type::Bool, false, move |a| Ok(Value::Bool(t == time_arg(&a[0])))),
        "Compare" => Func::new(vec![Type::Time], int(), false, move |a| {
            Ok(Value::int(match t.cmp(&time_arg(&a[0])) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }))
        }),
        "IsZero" => Func::new(vec![], Type::Bool, false, move |_| Ok(Value::Bool(t.is_zero()))),
        "Truncate" => Func::new(vec![dur()], tm, false, move |a| Ok(Value::Time(t.truncate(int_arg(&a[0]))))),
        "Round" => Func::new(vec![dur()], tm, false, move |a| Ok(Value::Time(t.round(int_arg(&a[0]))))),
        _ => return None,
    })
}

fn duration_method(d: i64, name: &str) -> Option<Func> {
    let f64t = Type::Float64;
    Some(match name {
        "String" => Func::new(vec![], s(), false, move |_| Ok(Value::string(gotime::duration_string(d)))),
        "Seconds" => Func::new(vec![], f64t, false, move |_| {
            let sec = d / gotime::SECOND;
            let nsec = d % gotime::SECOND;
            Ok(Value::Float(sec as f64 + nsec as f64 / 1e9))
        }),
        "Minutes" => Func::new(vec![], f64t, false, move |_| {
            let m = d / gotime::MINUTE;
            let nsec = d % gotime::MINUTE;
            Ok(Value::Float(m as f64 + nsec as f64 / (60.0 * 1e9)))
        }),
        "Hours" => Func::new(vec![], f64t, false, move |_| {
            let h = d / gotime::HOUR;
            let nsec = d % gotime::HOUR;
            Ok(Value::Float(h as f64 + nsec as f64 / (60.0 * 60.0 * 1e9)))
        }),
        "Milliseconds" => Func::new(vec![], int64(), false, move |_| Ok(Value::Int(d / gotime::MILLISECOND, IntKind::Int64))),
        "Microseconds" => Func::new(vec![], int64(), false, move |_| Ok(Value::Int(d / gotime::MICROSECOND, IntKind::Int64))),
        "Nanoseconds" => Func::new(vec![], int64(), false, move |_| Ok(Value::Int(d, IntKind::Int64))),
        "Abs" => Func::new(vec![], dur(), false, move |_| {
            Ok(Value::duration(if d >= 0 {
                d
            } else if d == i64::MIN {
                i64::MAX
            } else {
                -d
            }))
        }),
        "Round" => Func::new(vec![dur()], dur(), false, move |a| Ok(Value::duration(gotime::duration_round(d, int_arg(&a[0]))))),
        "Truncate" => {
            Func::new(vec![dur()], dur(), false, move |a| Ok(Value::duration(gotime::duration_truncate(d, int_arg(&a[0])))))
        }
        _ => return None,
    })
}

/// The exported fields of `histogram.FloatHistogram` that templates can read.
pub fn histogram_field(h: &model::histogram::FloatHistogram, name: &str) -> Option<Value> {
    Some(match name {
        "Count" => Value::Float(h.count),
        "Sum" => Value::Float(h.sum),
        "ZeroCount" => Value::Float(h.zero_count),
        "ZeroThreshold" => Value::Float(h.zero_threshold),
        "Schema" => Value::Int(h.schema as i64, IntKind::Int32),
        _ => return None,
    })
}
