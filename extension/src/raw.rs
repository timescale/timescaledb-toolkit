#![allow(non_camel_case_types)]

use pgx::*;

extension_sql!("\n\
        CREATE SCHEMA IF NOT EXISTS toolkit_experimental;\n\
    ",
    name = "create_experimental_schema",
    creates=[
        Type(bytea),
        Type(TimestampTz),
        Type(AnyElement),
        Type(tstzrange),
        Type(Interval),
        Type(regproc)
    ],
    bootstrap,
);

// TODO temporary holdover types while we migrate from nominal types to actual

macro_rules! raw_type {
    ($name:ident, $tyid: path, $arrayid: path) => {
        impl FromDatum for $name {
            const NEEDS_TYPID: bool = false;

            unsafe fn from_datum(datum: pg_sys::Datum, is_null: bool, _typoid: pg_sys::Oid) -> Option<Self>
            where
                Self: Sized {
                if is_null {
                    return None
                }
                Some(Self(datum))
            }
        }

        impl IntoDatum for $name {
            fn into_datum(self) -> Option<pg_sys::Datum> {
                Some(self.0)
            }
            fn type_oid() -> pg_sys::Oid {
                $tyid
            }
            fn array_type_oid() -> pg_sys::Oid {
                $arrayid
            }
        }

        impl From<pg_sys::Datum> for $name {
            fn from(d: pg_sys::Datum) -> Self {
                Self(d)
            }
        }

        impl Into<pg_sys::Datum> for $name {
            fn into(self) -> pg_sys::Datum {
                self.0
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct bytea(pub pg_sys::Datum);

raw_type!(bytea, pg_sys::BYTEAOID, pg_sys::BYTEAARRAYOID);

pub struct TimestampTz(pub pg_sys::Datum);

raw_type!(TimestampTz, pg_sys::TIMESTAMPTZOID, pg_sys::TIMESTAMPTZARRAYOID);

impl Into<pg_sys::TimestampTz> for TimestampTz {
    fn into(self) -> pg_sys::TimestampTz {
        self.0 as _
    }
}

impl From<pg_sys::TimestampTz> for TimestampTz {
    fn from(ts: pg_sys::TimestampTz) -> Self {
        Self(ts as _)
    }
}

pub struct AnyElement(pub pg_sys::Datum);

raw_type!(AnyElement, pg_sys::ANYELEMENTOID, pg_sys::ANYARRAYOID);

pub struct tstzrange(pub pg_sys::Datum);

raw_type!(tstzrange, pg_sys::TSTZRANGEOID, pg_sys::TSTZRANGEARRAYOID);

pub struct Interval(pub pg_sys::Datum);

raw_type!(Interval, pg_sys::INTERVALOID, pg_sys::INTERVALARRAYOID);

pub struct regproc(pub pg_sys::Datum);

raw_type!(regproc, pg_sys::REGPROCOID, pg_sys::REGPROCARRAYOID);