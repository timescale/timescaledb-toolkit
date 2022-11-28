import ctypes
import os

_cdll = ctypes.CDLL(os.path.join(
    os.getenv('CARGO_TARGET_DIR', 'target'),
    os.getenv('PROFILE', 'debug'),
    'libtimescaledb_toolkit_tdigest.so'))
_cdll.timescaledb_toolkit_tdigest_builder_with_size.restype = ctypes.c_void_p
_cdll.timescaledb_toolkit_tdigest_build.restype =  ctypes.c_void_p
_cdll.timescaledb_toolkit_tdigest_format_for_postgres.restype = ctypes.POINTER(ctypes.c_char)
_cdll.timescaledb_toolkit_tdigest_push.restype = None
_cdll.timescaledb_toolkit_tdigest_merge.restype = None
_cdll.timescaledb_toolkit_tdigest_builder_free.restype = None
_cdll.timescaledb_toolkit_tdigest_free.restype = None

# Wrapper classes use `real_pointer` to keep hold of the real pointer for as
# long as it needs to be released.
# We copy it to self.pointer to enforce use of `with` (as much as anything can be enforced in Python).
# Attempting to forego `with` results in `AttributeError`.

class TDigest:

    class Builder:
        def __init__(self, pointer):
            self.real_pointer = pointer

        def __enter__(self):
            self.pointer = self.real_pointer
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.__del__()
            self.real_pointer = None
            if 'pointer' in self.__dict__:
                del self.__dict__['pointer']

        def __del__(self):
            if self.real_pointer is not None:
                _cdll.timescaledb_toolkit_tdigest_builder_free(self.real_pointer)

        def with_size(size):
            return TDigest.Builder(ctypes.c_void_p(_cdll.timescaledb_toolkit_tdigest_builder_with_size(ctypes.c_size_t(size))))

        def push(self, value):
            _cdll.timescaledb_toolkit_tdigest_push(self.pointer, ctypes.c_double(value))

        def build(self):
            td = TDigest(ctypes.c_void_p(_cdll.timescaledb_toolkit_tdigest_build(self.pointer)))
            self.real_pointer = None
            del self.__dict__['pointer']
            return td

    def __init__(self, pointer):
        self.real_pointer = pointer

    def __enter__(self):
        self.pointer = self.real_pointer
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__del__()
        self.real_pointer = None
        if 'pointer' in self.__dict__:
            del self.__dict__['pointer']

    def __del__(self):
        if self.real_pointer is not None:
            _cdll.timescaledb_toolkit_tdigest_free(self.real_pointer)

    def format_for_postgres(self):
        buf = _cdll.timescaledb_toolkit_tdigest_format_for_postgres(self.pointer)
        s = ctypes.cast(buf, ctypes.c_char_p).value.decode('ascii')
        # TODO free(3) left as an exercise to the reader.  This is for GNU libc on Linux/amd64:
        ctypes.CDLL('libc.so.6').free(buf)
        return s

# Sample program which prints the expected output of the test_tdigest_io test.
def test():
    with TDigest.Builder.with_size(100) as builder:
        for value in range(1, 101):
            builder.push(value)
        with builder.build() as td:
            print(td.format_for_postgres())

if __name__ == '__main__':
    test()
