# Building aggregates safely

## Goals

1. Memory Safety:  no memory corruption, which can lead to corrupted results or worse.
2. Correctness:  mostly down to business logic, but to the extent a framework can help or hinder, at least do not hinder.
3. Robustness:  crashes are not as bad as incorrect results, but still undesirable.
4. Performance:  correct results returned quickly and without excessive resource consumption.
5. Developer productivity

We chose Rust because it gives us powerful tools to meet all 4 goals.
Unfortunately, we are not yet taking advantage of those tools.

Most of our aggregates naively call into unsafe code without any checks that
their invariants aren't invalidated.

## Next steps

I estimate two or three more weeks of effort to finish off the macros, plus
two hours or so of toil to convert each aggregate.

I suggest aggressively attacking experimental aggregates, and then converting
just one stabilized aggregate in a release before proceeding further,
out of an abundance of caution.

0. While at least some of us (me!) weren't looking, pgx added a new trait that
   may address some or all of our goals.  Evaluate that first.
1. Adapt at least a few more of our existing experimental aggregates to use
   the two new macros, in case that should turn up any show-stoppers.
2. Build out the rest of the macros:
   - finalfunc
   - serializefunc
   - deserializefunc
3. Finish building `aggregate` and `combine` macros:
   - support name override (vs. default of rust fn name)
   - support schema (haven't tested the non-schema case and hard-coded `toolkit_experimental` in one place)
   - immutable and parallel_safe (currently parsed but ignored)
   - copy the missing features from aggregate_builder (type assertions, test counters)
   - eliminate #body duplicate in aggregate.rs (see TODO)
   - fix bug about accepting any type named `Option` (require `std::option::Option`)
   - address clippy's complaints and other code cleanup
4. Nice to haves:
   - get rid of `#[sql_type]`
   - unduplicate the error! macro
   - tidy attribute-parsing error-handling (it's not wrong, just messy)

## Examples

### Illegal mutation

The PostgreSQL manual includes this big warning:

	Never modify the contents of a pass-by-reference input value. If you
	do so you are likely to corrupt on-disk data

Rust lets us express that in the type system such that code attempting to
modify that input does not compile.

Yet we pass those raw references into our business logic without such protection.

The ohlc bug was the inevitable result.

### Accidental unsafe

The primitives we currently use to build our aggregates encourage including
large blocks of code in unsafe blocks.  They don't require it; it is possible
to separate them.  But that's going against the grain.

In some cases we have business logic of high cyclomatic complexity and dozens
of lines all inside unsafe blocks.

Addressing this doesn't require building new primitives, but if we are, we
need to get this part right, too.

### Invalid cast

[I THINK pgx is able to put enough type information into the `CREATE FUNCTION`
and `CREATE AGGREGATE` such that postgresl can prevent this.  I THINK.
It still makes my hair stand on end, and many security disasters can be traced
back to "it's probably fine"... and it wasn't.]

This class of bug seems likely to be a developer productivity issue and less
likely to manifest in production, except our test coverage is not great and we
may ship a variant of an aggregate that isn't tested.

In any case:  we want a clear compiler error, not a mysterious crash (if we're
lucky) or mysteriously corrupt data (if we're unlucky) in testing.

What happens here is we implement a function accepting `pgx::Internal` and
then cast it to our internal type, and then later we bundle it with `CREATE
AGGREGATE` without any assurance that the types match:

```rust
fn foo_transition(state: pgx::Internal, value: Option<FooState>, fcinfo: pg_sys::FunctionCallInfo) {
    let state: Option<Inner<FooState>> = unsafe { state.to_inner() };
    // ...
}

fn bar_final(state: pgx::Internal, fcinfo: pg_sys::FunctionCallInfo) -> Option<Bar> {
    let state = Option<Inner<BarState>> = unsafe { state.to_inner() };
    // ...
}
```

```sql
CREATE AGGREGATE foo() (
    sfunc = foo_transition,
    stype = internal,
    finalfunc = bar_final,
);
```
