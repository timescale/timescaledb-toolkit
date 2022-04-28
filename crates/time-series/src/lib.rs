
use serde::{Deserialize, Serialize, ser::SerializeStruct};

use flat_serialize_macro::FlatSerializable;

use std::ffi::CStr;

#[derive(Clone, Copy, PartialEq, Debug, FlatSerializable)]
#[repr(C)]
pub struct TSPoint {
    pub ts: i64,
    pub val: f64,
}

#[derive(Debug, PartialEq)]
pub enum TSPointError {
    TimesEqualInterpolate,
}

impl TSPoint {
    pub fn interpolate_linear(&self, p2: &TSPoint, ts: i64) -> Result<f64, TSPointError> {
        if self.ts == p2.ts {
            return Err(TSPointError::TimesEqualInterpolate);
        }
        // using point slope form of a line iteratively y = y2 - y1 / (x2 - x1) * (x - x1) + y1
        let duration = (p2.ts - self.ts) as f64; // x2 - x1
        let dinterp = (ts - self.ts) as f64; // x - x1
        Ok((p2.val - self.val) * dinterp / duration + self.val)
    }
}

impl Serialize for TSPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        if serializer.is_human_readable() {
            // FIXME ugly hack to use postgres functions in an non-postgres library
            extern "C" {
                fn _ts_toolkit_encode_timestamptz(dt: i64, buf: &mut [u8; 128]);
            }
            let mut ts = [0; 128];
            unsafe {
                _ts_toolkit_encode_timestamptz(self.ts, &mut ts);
            }
            let end = ts.iter().position(|c| *c == 0).unwrap();
            let ts = CStr::from_bytes_with_nul(&ts[..end+1]).unwrap();
            let ts = ts.to_str().unwrap();
            let mut point = serializer.serialize_struct("TSPoint", 2)?;
            point.serialize_field("ts", &ts)?;
            point.serialize_field("val", &self.val)?;
            point.end()
        } else {
            let mut point = serializer.serialize_struct("TSPoint", 2)?;
            point.serialize_field("ts", &self.ts)?;
            point.serialize_field("val", &self.val)?;
            point.end()
        }
    }
}

impl<'de> Deserialize<'de> for TSPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {

        use std::fmt;
        use serde::de::{self, Visitor, SeqAccess, MapAccess};
        struct TsPointVisitor{ text_timestamp: bool }

        // FIXME ugly hack to use postgres functions in an non-postgres library
        extern "C" {
            // this is only going to be used to communicate with a rust lib we compile with this one
            #[allow(improper_ctypes)]
            fn _ts_toolkit_decode_timestamptz(text: &str) -> i64;
        }

        impl<'de> Visitor<'de> for TsPointVisitor {
            type Value = TSPoint;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct TSPoint")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<TSPoint, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let ts = if self.text_timestamp {
                    let text: &str = seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    unsafe {
                        _ts_toolkit_decode_timestamptz(text)
                    }
                } else {
                    seq.next_element()?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?
                };
                let val = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(TSPoint{ ts, val })
            }

            fn visit_map<V>(self, mut map: V) -> Result<TSPoint, V::Error>
            where
                V: MapAccess<'de>,
            {
                #[derive(Deserialize)]
                #[serde(field_identifier, rename_all = "lowercase")]
                enum Field { Ts, Val }
                let mut ts = None;
                let mut val = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Ts => {
                            if ts.is_some() {
                                return Err(de::Error::duplicate_field("ts"));
                            }
                            ts = if self.text_timestamp {
                                let text: &str = map.next_value()?;
                                unsafe {
                                    Some(_ts_toolkit_decode_timestamptz(text))
                                }
                            } else {
                                Some(map.next_value()?)
                            };
                        }
                        Field::Val => {
                            if val.is_some() {
                                return Err(de::Error::duplicate_field("val"));
                            }
                            val = Some(map.next_value()?);
                        }
                    }
                }
                let ts = ts.ok_or_else(|| de::Error::missing_field("ts"))?;
                let val = val.ok_or_else(|| de::Error::missing_field("val"))?;
                Ok(TSPoint{ ts, val })
            }
        }
        const FIELDS: &[&str] = &["ts", "val"];

        let visitor = TsPointVisitor { text_timestamp: deserializer.is_human_readable() };
        deserializer.deserialize_struct("TSPoint", FIELDS, visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_interpolate(){
        let p1 = TSPoint{ts: 1, val: 1.0};
        let p2 = TSPoint{ts: 3, val: 3.0};
        assert_eq!(p1.interpolate_linear(&p2, 2).unwrap(), 2.0);
        assert_eq!(p1.interpolate_linear(&p2, 3).unwrap(), 3.0);
        assert_eq!(p1.interpolate_linear(&p2, 4).unwrap(), 4.0);
        assert_eq!(p1.interpolate_linear(&p2, 0).unwrap(), 0.0);
        assert_eq!(p1.interpolate_linear(&p1, 2).unwrap_err(), TSPointError::TimesEqualInterpolate);
    }
}
