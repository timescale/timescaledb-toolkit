// cc -o tdigest tdigest.c $CARGO_TARGET_DIR/$PROFILE/libtimescaledb_toolkit_tdigest.a -lm -lpthread -ldl

// Sample program which prints the expected output of the test_tdigest_io test.

////////////////////////////////////////////////////////////////////////////////
// TODO Generate a header from tdigest-lib crate.

#include <sys/types.h>

struct TDigestBuilder;
struct TDigest;

// Return pointer to new TDigestBuilder.
// MUST NOT be passed to free(3).  Instead, pass to timescaledb_toolkit_tdigest_builder_free to
// discard or to timescaledb_toolkit_tdigest_build to convert to TDigest.
// Never returns NULL.
struct TDigestBuilder *
timescaledb_toolkit_tdigest_builder_with_size(size_t size);

void
timescaledb_toolkit_tdigest_push(struct TDigestBuilder *builder, double value);

void
timescaledb_toolkit_tdigest_merge(struct TDigestBuilder *builder, struct TDigestBuilder *other);

// Free a TDigestBuilder that has not been built.
// MUST NOT be passed NULL.
void
timescaledb_toolkit_tdigest_builder_free(struct TDigestBuilder *builder);

// Return pointer to new TDigest built from builder.
// builder MUST NOT be passed to timescaledb_toolkit_tdigest_builder_free .
struct TDigest *
timescaledb_toolkit_tdigest_build(struct TDigestBuilder *builder);

// Free a TDigest.
void
timescaledb_toolkit_tdigest_free(struct TDigest *td);

// Return pointer to null-terminated buffer containing ASCII serialization of TDigest suitable for
// use with postgresql INSERT.
// Free the buffer with free(3).
char *
timescaledb_toolkit_tdigest_format_for_postgres(struct TDigest *td);

////////////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <stdlib.h>

int
main()
{
    struct TDigestBuilder *builder = timescaledb_toolkit_tdigest_builder_with_size(100);
    double value;
    for (value = 1.0; value <= 100.0; value++) {
        timescaledb_toolkit_tdigest_push(builder, value);
    }

    struct TDigest *td = timescaledb_toolkit_tdigest_build(builder);
    char *formatted = timescaledb_toolkit_tdigest_format_for_postgres(td);
    printf("%s\n", formatted);
    free(formatted);

    timescaledb_toolkit_tdigest_free(td);

    return 0;
}
