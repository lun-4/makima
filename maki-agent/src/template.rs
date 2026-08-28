use std::borrow::Cow;
use std::env;
use std::path::Path;

use jiff::Timestamp;

pub fn env_vars() -> Vars {
    env_vars_for(&env::current_dir().unwrap_or_else(|_| ".".into()))
}

pub fn env_vars_for(cwd: &Path) -> Vars {
    let date = Timestamp::now().strftime("%Y-%m-%d").to_string();
    Vars::new()
        .set("{cwd}", cwd.to_string_lossy().into_owned())
        .set("{platform}", env::consts::OS)
        .set("{date}", date)
}

#[derive(Default, Clone)]
pub struct Vars(Vec<(&'static str, String)>);

impl Vars {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn set(mut self, key: &'static str, val: impl Into<String>) -> Self {
        self.0.push((key, val.into()));
        self
    }

    /// Appends (or overwrites) a single var on an existing set.
    pub fn with_custom(mut self, key: &'static str, val: impl Into<String>) -> Self {
        let val = val.into();
        if let Some(entry) = self.0.iter_mut().find(|(k, _)| *k == key) {
            entry.1 = val;
        } else {
            self.0.push((key, val));
        }
        self
    }

    pub fn apply<'a>(&self, s: &'a str) -> Cow<'a, str> {
        if self.0.is_empty() || !s.contains('{') {
            return Cow::Borrowed(s);
        }
        let mut out = s.to_string();
        for (k, v) in &self.0 {
            out = out.replace(k, v);
        }
        Cow::Owned(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    fn format_date(ts: Timestamp) -> String {
        ts.strftime("%Y-%m-%d").to_string()
    }

    #[test_case("{cwd} on {platform}", "/home on linux" ; "multiple_keys")]
    #[test_case("{x} and {x}", "42 and 42" ; "repeated_key")]
    #[test_case("no placeholders", "no placeholders" ; "no_placeholders")]
    fn apply(input: &str, expected: &str) {
        let vars = Vars::new()
            .set("{cwd}", "/home")
            .set("{platform}", "linux")
            .set("{x}", "42");
        assert_eq!(vars.apply(input).as_ref(), expected);
    }

    #[test_case(0,             "1970-01-01" ; "unix_epoch")]
    #[test_case(1_000_000_000, "2001-09-09" ; "billion_seconds")]
    #[test_case(1_740_700_800, "2025-02-28" ; "feb_28_non_leap")]
    #[test_case(1_709_164_800, "2024-02-29" ; "leap_day_2024")]
    fn format_date_cases(secs: i64, expected: &str) {
        let ts = Timestamp::from_second(secs).unwrap();
        assert_eq!(format_date(ts), expected);
    }

    #[test]
    fn env_vars_includes_date() {
        let vars = env_vars();
        let result = vars.apply("{date}");
        assert_ne!(result.as_ref(), "{date}");
    }
}
