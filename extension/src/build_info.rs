use pgrx::{iter::TableIterator, *};

/// Git commit this build came from. NULL if unknown.
#[pg_extern(stable, parallel_safe, schema = "toolkit_experimental")]
pub fn get_toolkit_git_commit() -> TableIterator<
    'static,
    (
        name!(commit_tag, Option<&'static str>),
        name!(commit_hash, Option<&'static str>),
    ),
> {
    TableIterator::once((
        option_env!("TOOLKIT_GIT_COMMIT_TAG"),
        option_env!("TOOLKIT_GIT_COMMIT_HASH"),
    ))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::*;
    use pgrx_macros::pg_test;

    #[pg_test]
    fn get_toolkit_git_commit_returns_one_row() {
        let n = Spi::get_one::<i64>(
            "SELECT count(*) FROM toolkit_experimental.get_toolkit_git_commit()",
        );
        assert_eq!(n, Ok(Some(1)));
    }
}
