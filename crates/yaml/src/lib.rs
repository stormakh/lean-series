//! YAML for Prometheus configs with the behaviour of go.yaml.in/yaml/v2, which upstream config uses
//! (`yaml.UnmarshalStrict` to load, `yaml.Marshal` for `/api/v1/status/config`).
//!
//! Decode: `yaml::unmarshal_strict::<T>(text)` or `yaml::unmarshal_into(text, &mut value, strict)`. Types implement
//! [`Unmarshal`]; Go structs implement [`Struct`] with the [`yaml_struct!`] macro and then either
//! [`impl_plain_struct!`] (no custom logic) or a hand-written `Unmarshal` that mirrors the Go `UnmarshalYAML`
//! (`d.custom(|d| { *self = Defaults; d.inner_struct(n, self)?; self.validate() })`).
//!
//! Encode: `yaml::marshal(&value)`; types implement [`Marshal`].
//!
//! Parsing is done by yaml-rust2 (YAML 1.2 syntax); scalar resolution (YAML 1.1 booleans, octal, ...) is
//! go-yaml's.
pub mod decode;
pub mod emitter;
pub mod encode;
pub mod node;
pub mod rawbytes;
pub mod resolve;
pub mod types;
pub mod url;
pub mod value;

pub use decode::{Decoder, Error, Field, MapKey, Result, Struct, Unmarshal, go_quote};
pub use encode::{Marshal, emit, format_float_g, marshal, marshal_struct, push_field};
pub use node::{Kind, Node};
pub use types::{Base2Bytes, Duration, MARSHAL_SECRET_VALUE, Secret, Url, parse_base2_bytes, parse_duration, parse_duration_allow_negative};
pub use value::Value;

/// go-yaml `Unmarshal` into a fresh `T::default()`.
pub fn unmarshal<T: Unmarshal + Default>(text: &str) -> Result<T> {
    let mut v = T::default();
    decode::decode_into(text, &mut v, false)?;
    Ok(v)
}

/// go-yaml `UnmarshalStrict` into a fresh `T::default()`.
pub fn unmarshal_strict<T: Unmarshal + Default>(text: &str) -> Result<T> {
    let mut v = T::default();
    decode::decode_into(text, &mut v, true)?;
    Ok(v)
}

/// go-yaml `Unmarshal`/`UnmarshalStrict` into an existing value (fields not in the text keep their values).
pub fn unmarshal_into<T: Unmarshal + Default>(text: &str, out: &mut T, strict: bool) -> Result<()> {
    decode::decode_into(text, out, strict)
}

/// Implements [`Struct`] for a Go struct: its Go type name for errors and its fields in declaration order.
///
/// ```ignore
/// yaml::yaml_struct!(BasicAuth, "config.plain", [
///     (username: "username", omitempty)
///     (password: "password", omitempty)
///     (inline proxy)            // a `yaml:",inline"` struct field (must implement Struct)
///     (follow_redirects: "follow_redirects")
///     (ptrs rules: "rules" as "[]*relabel.Config", omitempty)  // a Go `[]*T` field as Vec<T> (see
///                                                              // `Decoder::decode_ptr_vec`)
/// ]);
/// ```
#[macro_export]
macro_rules! yaml_struct {
    ($ty:ty, $name:expr, [ $( $entry:tt )* ]) => {
        impl $crate::Struct for $ty {
            fn type_name(&self) -> &'static str {
                $name
            }
            #[allow(unused_variables)]
            fn has_field(&self, k: &str) -> bool {
                $( $crate::__yaml_has!(self, k, $entry); )*
                false
            }
            #[allow(unused_variables)]
            fn field(&mut self, k: &str) -> Option<&mut dyn $crate::Field> {
                $( $crate::__yaml_field!(self, k, $entry); )*
                None
            }
            #[allow(unused_variables)]
            fn marshal_fields(&self, out: &mut Vec<($crate::Value, $crate::Value)>) -> std::result::Result<(), String> {
                $( $crate::__yaml_marshal!(self, out, $entry); )*
                Ok(())
            }
            fn fields_zero(&self) -> bool {
                $( $crate::__yaml_zero!(self, $entry); )*
                true
            }
            #[allow(unused_variables)]
            fn decode_key(&mut self, k: &str, n: &$crate::Node, d: &mut $crate::Decoder) -> Option<$crate::Result<bool>> {
                $( $crate::__yaml_decode_key!(self, k, n, d, $entry); )*
                None
            }
        }
    };
}

/// Implements [`Unmarshal`] and [`Marshal`] for a [`Struct`] without custom logic (a Go struct without
/// `UnmarshalYAML`/`MarshalYAML`).
#[macro_export]
macro_rules! impl_plain_struct {
    ($ty:ty) => {
        impl $crate::Unmarshal for $ty {
            fn unmarshal_yaml(&mut self, n: &$crate::Node, d: &mut $crate::Decoder) -> $crate::Result<bool> {
                d.decode_struct(n, self)
            }
            fn go_type() -> String {
                $crate::Struct::type_name(&<$ty>::default()).to_string()
            }
        }
        impl $crate::Marshal for $ty {
            fn marshal_yaml(&self) -> std::result::Result<$crate::Value, String> {
                $crate::marshal_struct(self)
            }
            fn is_zero(&self) -> bool {
                $crate::Struct::fields_zero(self)
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __yaml_decode_key {
    ($s:ident, $k:ident, $n:ident, $d:ident, (ptrs $f:ident : $key:literal as $gt:literal $(, $o:ident)?)) => {
        if $k == $key {
            return Some($d.decode_ptr_vec($n, &mut $s.$f, $key, $gt));
        }
    };
    ($s:ident, $k:ident, $n:ident, $d:ident, ($f:ident : $key:literal $(, $o:ident)?)) => {};
    ($s:ident, $k:ident, $n:ident, $d:ident, (inline $f:ident)) => {
        if $crate::Struct::has_field(&$s.$f, $k) {
            if let Some(r) = $crate::Struct::decode_key(&mut $s.$f, $k, $n, $d) {
                return Some(r);
            }
        }
    };
}
#[doc(hidden)]
#[macro_export]
macro_rules! __yaml_has {
    ($s:ident, $k:ident, (ptrs $f:ident : $key:literal as $gt:literal $(, $o:ident)?)) => {
        if $k == $key {
            return true;
        }
    };
    ($s:ident, $k:ident, ($f:ident : $key:literal $(, $o:ident)?)) => {
        if $k == $key {
            return true;
        }
    };
    ($s:ident, $k:ident, (inline $f:ident)) => {
        if $crate::Struct::has_field(&$s.$f, $k) {
            return true;
        }
    };
}
#[doc(hidden)]
#[macro_export]
macro_rules! __yaml_field {
    ($s:ident, $k:ident, (ptrs $f:ident : $key:literal as $gt:literal $(, $o:ident)?)) => {};
    ($s:ident, $k:ident, ($f:ident : $key:literal $(, $o:ident)?)) => {
        if $k == $key {
            return Some(&mut $s.$f as &mut dyn $crate::Field);
        }
    };
    ($s:ident, $k:ident, (inline $f:ident)) => {
        if $crate::Struct::has_field(&$s.$f, $k) {
            return $crate::Struct::field(&mut $s.$f, $k);
        }
    };
}
#[doc(hidden)]
#[macro_export]
macro_rules! __yaml_marshal {
    ($s:ident, $out:ident, (ptrs $f:ident : $key:literal as $gt:literal, omitempty)) => {
        $crate::push_field($out, $key, &$s.$f, true)?;
    };
    ($s:ident, $out:ident, (ptrs $f:ident : $key:literal as $gt:literal)) => {
        $crate::push_field($out, $key, &$s.$f, false)?;
    };
    ($s:ident, $out:ident, ($f:ident : $key:literal, omitempty)) => {
        $crate::push_field($out, $key, &$s.$f, true)?;
    };
    ($s:ident, $out:ident, ($f:ident : $key:literal)) => {
        $crate::push_field($out, $key, &$s.$f, false)?;
    };
    ($s:ident, $out:ident, (inline $f:ident)) => {
        $crate::Struct::marshal_fields(&$s.$f, $out)?;
    };
}
#[doc(hidden)]
#[macro_export]
macro_rules! __yaml_zero {
    ($s:ident, (ptrs $f:ident : $key:literal as $gt:literal $(, $o:ident)?)) => {
        if !$crate::Marshal::is_zero(&$s.$f) {
            return false;
        }
    };
    ($s:ident, ($f:ident : $key:literal $(, $o:ident)?)) => {
        if !$crate::Marshal::is_zero(&$s.$f) {
            return false;
        }
    };
    ($s:ident, (inline $f:ident)) => {
        if !$crate::Struct::fields_zero(&$s.$f) {
            return false;
        }
    };
}
