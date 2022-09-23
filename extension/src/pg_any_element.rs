use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    mem::size_of,
};

use pgx::*;

use pg_sys::{Datum, Oid};

use crate::datum_utils::{deep_copy_datum, DatumHashBuilder};

// Unable to implement PartialEq for AnyElement, so creating a local copy
pub struct PgAnyElement {
    datum: Datum,
    typoid: Oid,
}

impl PgAnyElement {
    // pub fn from_datum_clone(datum : Datum, typoid : Oid) -> PgAnyElement {
    //     PgAnyElement {
    //         datum : unsafe{deep_copy_datum(datum, typoid)},
    //         typoid
    //     }
    // }

    pub fn deep_copy_datum(&self) -> Datum {
        unsafe { deep_copy_datum(self.datum, self.typoid) }
    }
}

impl PartialEq for PgAnyElement {
    #[allow(clippy::field_reassign_with_default)]
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            if self.typoid != other.typoid {
                false
            } else {
                // TODO JOSH can we avoid the type cache lookup here
                let typ = self.typoid;
                let tentry = pg_sys::lookup_type_cache(typ, pg_sys::TYPECACHE_EQ_OPR_FINFO as _);

                let flinfo = if (*tentry).eq_opr_finfo.fn_addr.is_some() {
                    &(*tentry).eq_opr_finfo
                } else {
                    pgx::error!("no equality function");
                };

                let size = size_of::<pg_sys::FunctionCallInfoBaseData>()
                    + size_of::<pg_sys::NullableDatum>() * 2;
                let mut info = pg_sys::palloc0(size) as pg_sys::FunctionCallInfo;

                (*info).flinfo = flinfo as *const pg_sys::FmgrInfo as *mut pg_sys::FmgrInfo;
                (*info).context = std::ptr::null_mut();
                (*info).resultinfo = std::ptr::null_mut();
                (*info).fncollation = (*tentry).typcollation;
                (*info).isnull = false;
                (*info).nargs = 2;

                (*info).args.as_mut_slice(2)[0] = pg_sys::NullableDatum {
                    value: self.datum,
                    isnull: false,
                };
                (*info).args.as_mut_slice(2)[1] = pg_sys::NullableDatum {
                    value: other.datum,
                    isnull: false,
                };
                (*(*info).flinfo).fn_addr.unwrap()(info) != Datum::from(0)
            }
        }
    }
}

impl Eq for PgAnyElement {}

impl Hash for PgAnyElement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.datum.hash(state);
    }
}

impl From<(Datum, Oid)> for PgAnyElement {
    fn from(other: (Datum, Oid)) -> Self {
        let (datum, typoid) = other;
        PgAnyElement { datum, typoid }
    }
}

impl From<AnyElement> for PgAnyElement {
    fn from(other: AnyElement) -> Self {
        PgAnyElement {
            datum: other.datum(),
            typoid: other.oid(),
        }
    }
}

pub struct PgAnyElementHashMap<V>(pub(crate) HashMap<PgAnyElement, V, DatumHashBuilder>);

impl<V> PgAnyElementHashMap<V> {
    pub fn new(typoid: Oid, collation: Option<Oid>) -> Self {
        PgAnyElementHashMap(HashMap::with_hasher(unsafe {
            DatumHashBuilder::from_type_id(typoid, collation)
        }))
    }

    pub(crate) fn with_hasher(hasher: DatumHashBuilder) -> Self {
        PgAnyElementHashMap(HashMap::with_hasher(hasher))
    }

    pub fn typoid(&self) -> Oid {
        self.0.hasher().type_id
    }

    // Passthroughs
    pub fn contains_key(&self, k: &PgAnyElement) -> bool {
        self.0.contains_key(k)
    }
    pub fn get(&self, k: &PgAnyElement) -> Option<&V> {
        self.0.get(k)
    }
    pub fn get_mut(&mut self, k: &PgAnyElement) -> Option<&mut V> {
        self.0.get_mut(k)
    }
    pub(crate) fn hasher(&self) -> &DatumHashBuilder {
        self.0.hasher()
    }
    pub fn insert(&mut self, k: PgAnyElement, v: V) -> Option<V> {
        self.0.insert(k, v)
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn remove(&mut self, k: &PgAnyElement) -> Option<V> {
        self.0.remove(k)
    }
}
