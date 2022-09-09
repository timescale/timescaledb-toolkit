//! Utilities for working with durations. Parsing of duration units is intended to match how
//! PostgreSQL parses duration units. Currently units longer than an hour are unsupported since
//! the length of days varies when in a timezone with daylight savings time.

// Canonical PostgreSQL units: https://github.com/postgres/postgres/blob/b76fb6c2a99eb7d49f96e56599fef1ffc1c134c9/src/include/utils/datetime.h#L48-L60
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DurationUnit {
    // units should be ordered smallest -> largest
    Microsec,
    Millisec,
    Second,
    Minute,
    Hour,
}

impl DurationUnit {
    fn microseconds(self) -> u32 {
        match self {
            Self::Microsec => 1,
            Self::Millisec => 1000,
            Self::Second => 1_000_000,
            Self::Minute => 60_000_000,
            Self::Hour => 3_600_000_000,
        }
    }

    /// Convert `amount` of a unit to another unit.
    pub fn convert_unit(self, amount: f64, to: Self) -> f64 {
        let microseconds = amount * (self.microseconds() as f64);
        microseconds / (to.microseconds() as f64)
    }

    /// Tries to get a duration unit from a string, returning `None` if no known unit matched.
    pub fn from_str(s: &str) -> Option<Self> {
        // Aliases for canonical units: https://github.com/postgres/postgres/blob/b76fb6c2a99eb7d49f96e56599fef1ffc1c134c9/src/backend/utils/adt/datetime.c#L187-L247
        match s.to_lowercase().as_str() {
            "usecond" | "microsecond" | "microseconds" | "microsecon" | "us" | "usec"
            | "useconds" | "usecs" => Some(Self::Microsec),
            "msecond" | "millisecond" | "milliseconds" | "millisecon" | "ms" | "msec"
            | "mseconds" | "msecs" => Some(Self::Millisec),
            "second" | "s" | "sec" | "seconds" | "secs" => Some(Self::Second),
            "minute" | "m" | "min" | "mins" | "minutes" => Some(Self::Minute),
            "hour" | "hours" | "h" | "hr" | "hrs" => Some(Self::Hour),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn convert_unit() {
        let load_time_secs = 75.0;
        let load_time_mins =
            DurationUnit::convert_unit(DurationUnit::Second, load_time_secs, DurationUnit::Minute);
        assert_eq!(load_time_mins, 1.25);
    }

    #[test]
    fn parse_unit() {
        assert_eq!(
            DurationUnit::from_str("usecs"),
            Some(DurationUnit::Microsec)
        );
        assert_eq!(DurationUnit::from_str("MINUTE"), Some(DurationUnit::Minute));
        assert_eq!(
            DurationUnit::from_str("MiLlIsEcOn"),
            Some(DurationUnit::Millisec)
        );
        assert_eq!(DurationUnit::from_str("pahar"), None);
        assert_eq!(DurationUnit::from_str(""), None);
    }
}
