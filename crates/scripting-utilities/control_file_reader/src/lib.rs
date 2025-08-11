/// Code to extract info from `timescaledb_toolkit.control`
/// This crate exists so we have a single source of truth for the format.
use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// extract the current version from the control file
pub fn get_current_version(control_file: &str) -> Result<String> {
    get_field_val(control_file, "version").map(|v| v.to_string())
}

/// extract the list of versions we're upgradeable-from from the control file
pub fn get_upgradeable_from(control_file: &str) -> Result<Vec<String>> {
    // versions is a comma-delimited list of versions
    let versions = get_field_val(control_file, "upgradeable_from")?;
    let versions = versions
        .split_terminator(',')
        .map(|version| version.trim().to_string())
        .collect();
    Ok(versions)
}

/// find a `<field name> = '<field value>'` in `file` and extract `<field value>`
pub fn get_field_val<'a>(file: &'a str, field_name: &str) -> Result<&'a str> {
    file.lines()
        .filter(|line| line.starts_with(field_name) || line.starts_with(&format!("# {field_name}")))
        .map(get_quoted_field)
        .next()
        .ok_or(Error::FieldNotFound)
        .and_then(|e| e) // flatten the nested results
}

// given a `<field name> = '<field value>'` extract `<field value>`
pub fn get_quoted_field(line: &str) -> Result<&str> {
    let quoted = line.split('=').nth(1).ok_or(Error::NoValue)?;

    quoted
        .trim_start()
        .split_terminator('\'')
        .find(|s| !s.is_empty())
        .ok_or(Error::UnquotedValue)
}

pub enum Error {
    FieldNotFound,
    NoValue,
    UnquotedValue,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::FieldNotFound => write!(f, "cannot read field"),
            Self::NoValue => write!(f, "cannot find value"),
            Self::UnquotedValue => write!(f, "unquoted value"),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
