// There is no safety here:  it's all in the hands of the caller, bless their heart.
#![allow(clippy::missing_safety_doc)]

#[no_mangle]
pub extern "C" fn timescaledb_toolkit_tdigest_builder_with_size(
    size: usize,
) -> Box<tdigest::Builder> {
    Box::new(tdigest::Builder::with_size(size))
}

#[no_mangle]
pub unsafe extern "C" fn timescaledb_toolkit_tdigest_push(
    builder: *mut tdigest::Builder,
    value: f64,
) {
    (*builder).push(value)
}

// TODO Don't abort the process if `builder` and `other` weren't created with the same size.
#[no_mangle]
pub unsafe extern "C" fn timescaledb_toolkit_tdigest_merge(
    builder: *mut tdigest::Builder,
    other: Box<tdigest::Builder>,
) {
    let other = *other;
    (*builder).merge(other)
}

#[no_mangle]
pub extern "C" fn timescaledb_toolkit_tdigest_builder_free(_: Box<tdigest::Builder>) {}

#[no_mangle]
pub extern "C" fn timescaledb_toolkit_tdigest_build(
    mut builder: Box<tdigest::Builder>,
) -> Box<tdigest::TDigest> {
    Box::new(builder.build())
}

#[no_mangle]
pub extern "C" fn timescaledb_toolkit_tdigest_free(_: Box<tdigest::TDigest>) {}

// TODO Messy, but good enough to experiment with.  We might want to
// into_raw_parts the String and offer a transparent struct containing pointer
// to and size of the buffer, with a ts_tk_tdigest_string_free taking it back
// and releasing it.  That also avoids one copy.
#[no_mangle]
pub unsafe extern "C" fn timescaledb_toolkit_tdigest_format_for_postgres(
    td: *const tdigest::TDigest,
) -> *mut libc::c_char {
    let s = (*td).format_for_postgres();
    let buf = libc::malloc(s.len() + 1);
    libc::memcpy(buf, s.as_ptr() as *const libc::c_void, s.len());
    let buf = buf as *mut libc::c_char;
    let r = std::slice::from_raw_parts_mut(buf, s.len() + 1);
    r[s.len()] = 0;
    buf
}
