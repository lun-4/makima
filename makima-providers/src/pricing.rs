//! What a turn, and a whole session, cost.
//!
//! Rates live on the model. Turning them into a bill happens here: the
//! wall-clock schedules that scale them, and how a stored session's total comes
//! back. The rule everything else leans on is that a turn is priced once, when
//! it runs, and that number is what gets stored, summed and shown. History is
//! never re-priced, because rates move.

use std::collections::HashMap;
use std::fmt;

use jiff::Timestamp;
use jiff::civil::Weekday;
use jiff::tz::Offset;
use makima_storage::sessions::StoredTokenUsage;
use serde::{Deserialize, Serialize};

use crate::model::{Model, TokenUsage};
use crate::registry::ProviderRegistry;

const HOURS_PER_DAY: u8 = 24;
/// Multiplier of a provider that bills the same rate around the clock.
pub(crate) const FLAT_RATE: f64 = 1.0;
const NO_WINDOWS_ERR: &str = "a schedule with no windows never applies; drop it instead";
const NO_SURCHARGE_ERR: &str =
    "descriptors quote the off-peak rates, so a schedule multiplier must be above 1";
const OUTSIDE_DAY_ERR: &str = "lies outside the day";
const EMPTY_WINDOW_ERR: &str = "covers no time at all";
const WRAPPING_WEEKDAYS_ERR: &str =
    "wraps past midnight, which a weekdays-only schedule cannot bill";

/// Rates that move with the wall clock. DeepSeek doubles everything during its
/// peak UTC hours, so its model descriptors quote the off-peak rates and carry
/// this schedule, which scales the bill inside its windows.
///
/// One multiplier is enough while providers move all four rates together.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ScheduleFields")]
pub struct PricingSchedule {
    windows: Vec<PricingWindow>,
    weekdays_only: bool,
    multiplier: f64,
}

/// Half-open `[start, end)` in whole UTC hours. `start > end` wraps past
/// midnight, so a window never needs splitting in two.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricingWindow {
    start: u8,
    end: u8,
}

impl PricingWindow {
    fn contains(&self, hour: u8) -> bool {
        if self.start < self.end {
            hour >= self.start && hour < self.end
        } else {
            hour >= self.start || hour < self.end
        }
    }
}

/// `01:00-04:00`, the shape provider docs quote peak hours in.
impl fmt::Display for PricingWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:02}:00-{:02}:00", self.start, self.end)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleFields {
    windows: Vec<PricingWindow>,
    #[serde(default)]
    weekdays_only: bool,
    multiplier: f64,
}

impl TryFrom<ScheduleFields> for PricingSchedule {
    type Error = String;

    fn try_from(fields: ScheduleFields) -> Result<Self, String> {
        if fields.windows.is_empty() {
            return Err(NO_WINDOWS_ERR.into());
        }
        if fields.multiplier.is_nan() || fields.multiplier <= FLAT_RATE {
            return Err(NO_SURCHARGE_ERR.into());
        }
        for window in &fields.windows {
            if window.start >= HOURS_PER_DAY || window.end > HOURS_PER_DAY {
                return Err(format!("window {window} {OUTSIDE_DAY_ERR}"));
            }
            if window.start == window.end {
                return Err(format!("window {window} {EMPTY_WINDOW_ERR}"));
            }
            if fields.weekdays_only && window.start > window.end {
                return Err(format!("window {window} {WRAPPING_WEEKDAYS_ERR}"));
            }
        }
        Ok(Self {
            windows: fields.windows,
            weekdays_only: fields.weekdays_only,
            multiplier: fields.multiplier,
        })
    }
}

impl PricingSchedule {
    /// DeepSeek publishes the days in UTC next to the hours, so the UTC weekday
    /// is the rule itself and not an approximation. Their Chinese page words it
    /// as Monday to Friday 09:00-18:00 Beijing, the same instants on the same
    /// days, because the windows sit inside one Beijing date.
    pub(crate) fn multiplier_at(&self, at: Timestamp) -> f64 {
        let at = Offset::UTC.to_datetime(at);
        if self.weekdays_only && matches!(at.weekday(), Weekday::Saturday | Weekday::Sunday) {
            return FLAT_RATE;
        }
        if self.windows.iter().any(|w| w.contains(at.hour() as u8)) {
            self.multiplier
        } else {
            FLAT_RATE
        }
    }
}

/// `2x during 01:00-04:00, 06:00-10:00 UTC, Mon-Fri`. Docs and hints render
/// this rather than restate the hours in prose that drifts.
impl fmt::Display for PricingSchedule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}x during ", self.multiplier)?;
        for (i, window) in self.windows.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{window}")?;
        }
        f.write_str(" UTC")?;
        if self.weekdays_only {
            f.write_str(", Mon-Fri")?;
        }
        Ok(())
    }
}

/// The bill a stored session ran up. Status bar, `/usage`, ACP and headless all
/// come here, so they cannot disagree about the same session.
///
/// Turns record what they paid, so summing those is the truth. What was written
/// before that kept counters only, and its estimate against today's table is
/// settled into the entry here, once: after a later turn merges its own cost in,
/// the counters no longer say which of them was already paid for, so estimating
/// again would drop everything the entry had before. Counters with no breakdown
/// get one seeded, so there is always somewhere to settle into.
///
/// `None` when nothing here is priced (oauth, local models), so callers show no
/// cost instead of a made up "$0.000".
pub fn settle_session(
    total: &TokenUsage,
    by_model: &mut HashMap<String, StoredTokenUsage>,
    current: &Model,
    fast: bool,
    providers: &ProviderRegistry,
) -> Option<f64> {
    if by_model.is_empty() && *total != TokenUsage::default() {
        by_model.insert(current.id.clone(), total.billed(None));
    }
    for (id, usage) in by_model.iter_mut() {
        usage.cost = model_cost(id, usage, current, fast, providers);
    }
    by_model
        .values()
        .filter_map(|usage| usage.cost)
        .reduce(|total, cost| total + cost)
}

/// One model's slice of [`settle_session`], as `/usage` breaks it down per row.
/// Usage is keyed by bare model id, so resolving it needs the session's
/// provider put back in front.
pub fn model_cost(
    id: &str,
    usage: &StoredTokenUsage,
    current: &Model,
    fast: bool,
    providers: &ProviderRegistry,
) -> Option<f64> {
    if let Some(cost) = usage.cost {
        return Some(cost);
    }
    if id == current.id {
        return current.list_cost(&(*usage).into(), fast);
    }
    providers
        .model(id)
        .or_else(|_| providers.model(&format!("{}/{}", current.provider, id)))
        .ok()?
        .list_cost(&(*usage).into(), fast)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{FastPricing, FastSupport, ModelInfo, ModelPricing};
    use crate::test_util::offline_registry;
    use test_case::test_case;

    const SECONDS_PER_MINUTE: i64 = 60;
    const SECONDS_PER_HOUR: i64 = 60 * SECONDS_PER_MINUTE;
    const SECONDS_PER_DAY: i64 = HOURS_PER_DAY as i64 * SECONDS_PER_HOUR;
    const PEAK: f64 = 2.0;
    const DAYS_SINCE_EPOCH: i64 = 20_000;
    const PEAK_WINDOWS: &[(u8, u8)] = &[(1, 4), (6, 10)];
    const WRAPPING_WINDOW: &[(u8, u8)] = &[(22, 2)];
    const UNTIL_MIDNIGHT_WINDOW: &[(u8, u8)] = &[(22, HOURS_PER_DAY)];
    const WHOLE_DAY_WINDOW: &[(u8, u8)] = &[(0, HOURS_PER_DAY)];
    const LAST_MINUTE: i64 = 59;
    const LAST_SECOND: i64 = 59;

    const CURRENT: &str = "current";
    const INPUT_RATE: f64 = 3.0;
    const UNPRICED: f64 = 0.0;
    const ONE_MILLION: u32 = 1_000_000;
    /// [`ONE_MILLION`] input tokens at [`INPUT_RATE`].
    const LIST_PRICE: f64 = 3.0;
    const RECORDED: f64 = 0.5;
    const UNRESOLVABLE: &str = "a-model-no-table-has-ever-heard-of";
    const ALSO_UNRESOLVABLE: &str = "another-model-no-table-has-ever-heard-of";
    const FAST_INPUT_RATE: f64 = 12.0;
    /// [`ONE_MILLION`] input tokens at [`FAST_INPUT_RATE`].
    const FAST_LIST_PRICE: f64 = 12.0;
    const PROVIDER: &str = "priced";
    const SIBLING: &str = "sibling";
    const SIBLING_INPUT_RATE: f64 = 7.0;

    fn schedule(windows: &[(u8, u8)], weekdays_only: bool) -> Result<PricingSchedule, String> {
        PricingSchedule::try_from(ScheduleFields {
            windows: windows
                .iter()
                .map(|&(start, end)| PricingWindow { start, end })
                .collect(),
            weekdays_only,
            multiplier: PEAK,
        })
    }

    fn utc(day: i64, hour: i64, minute: i64, second: i64) -> Timestamp {
        Timestamp::from_second(
            day * SECONDS_PER_DAY + hour * SECONDS_PER_HOUR + minute * SECONDS_PER_MINUTE + second,
        )
        .expect("timestamp in range")
    }

    fn model(id: &str, input_rate: f64) -> Model {
        Model {
            pricing: ModelPricing {
                input: input_rate,
                ..ModelPricing::ZERO
            },
            ..Model::from_spec(&format!("{PROVIDER}/{id}")).unwrap()
        }
    }

    fn stored(cost: Option<f64>) -> StoredTokenUsage {
        StoredTokenUsage {
            input: ONE_MILLION,
            cost,
            ..Default::default()
        }
    }

    fn breakdown(entries: &[(&str, Option<f64>)]) -> HashMap<String, StoredTokenUsage> {
        entries
            .iter()
            .map(|(id, cost)| ((*id).to_owned(), stored(*cost)))
            .collect()
    }

    /// Windows are half-open down to the second, since an off-by-one bills a
    /// whole hour at the wrong rate. Every case also runs on a day before the
    /// epoch, where the timestamp is negative and must not wrap into another
    /// window.
    #[test_case(PEAK_WINDOWS, 0, LAST_MINUTE, LAST_SECOND, FLAT_RATE ; "last_second_before_the_start")]
    #[test_case(PEAK_WINDOWS, 1, 0, 0, PEAK                          ; "first_second_of_a_window")]
    #[test_case(PEAK_WINDOWS, 3, LAST_MINUTE, LAST_SECOND, PEAK      ; "last_second_inside")]
    #[test_case(PEAK_WINDOWS, 4, 0, 0, FLAT_RATE                     ; "first_second_after_the_end")]
    #[test_case(PEAK_WINDOWS, 7, 15, 0, PEAK                         ; "second_window")]
    #[test_case(PEAK_WINDOWS, 23, 0, 0, FLAT_RATE                    ; "after_the_last_window")]
    #[test_case(WRAPPING_WINDOW, 23, 0, 0, PEAK                      ; "wrapping_before_midnight")]
    #[test_case(WRAPPING_WINDOW, 0, 0, 0, PEAK                       ; "wrapping_across_midnight")]
    #[test_case(WRAPPING_WINDOW, 21, LAST_MINUTE, LAST_SECOND, FLAT_RATE ; "wrapping_start_is_half_open")]
    #[test_case(WRAPPING_WINDOW, 2, 0, 0, FLAT_RATE                  ; "wrapping_end_is_half_open")]
    #[test_case(UNTIL_MIDNIGHT_WINDOW, 23, LAST_MINUTE, LAST_SECOND, PEAK ; "ends_at_midnight")]
    #[test_case(UNTIL_MIDNIGHT_WINDOW, 0, 0, 0, FLAT_RATE            ; "ending_at_midnight_does_not_wrap")]
    #[test_case(WHOLE_DAY_WINDOW, 0, 0, 0, PEAK                      ; "whole_day_starts_at_midnight")]
    #[test_case(WHOLE_DAY_WINDOW, 23, LAST_MINUTE, LAST_SECOND, PEAK ; "whole_day_never_leaves_peak")]
    fn windows_price_by_the_utc_clock(
        windows: &[(u8, u8)],
        hour: i64,
        minute: i64,
        second: i64,
        expected: f64,
    ) {
        let schedule = schedule(windows, false).unwrap();
        for day in [DAYS_SINCE_EPOCH, -DAYS_SINCE_EPOCH] {
            assert_eq!(
                schedule.multiplier_at(utc(day, hour, minute, second)),
                expected,
                "day {day}"
            );
        }
    }

    /// The weekend bills the very same hour a second way, so the day has to be
    /// read before the clock. `07:00` sits inside [`PEAK_WINDOWS`], `12:00`
    /// outside every one of them.
    #[test_case("2024-01-01T07:00:00Z", PEAK      ; "monday_inside_a_window")]
    #[test_case("2024-01-06T07:00:00Z", FLAT_RATE ; "saturday_inside_the_same_window")]
    #[test_case("2024-01-07T07:00:00Z", FLAT_RATE ; "sunday_inside_the_same_window")]
    #[test_case("2024-01-01T12:00:00Z", FLAT_RATE ; "monday_outside_every_window")]
    fn a_weekdays_only_schedule_leaves_the_weekend_alone(at: &str, expected: f64) {
        let schedule = schedule(PEAK_WINDOWS, true).unwrap();
        let at = at.parse().expect("a valid timestamp");
        assert_eq!(schedule.multiplier_at(at), expected);
    }

    #[test]
    fn schedules_render_the_hours_and_days_they_bill() {
        assert_eq!(
            schedule(PEAK_WINDOWS, false).unwrap().to_string(),
            "2x during 01:00-04:00, 06:00-10:00 UTC"
        );
        assert_eq!(
            schedule(PEAK_WINDOWS, true).unwrap().to_string(),
            "2x during 01:00-04:00, 06:00-10:00 UTC, Mon-Fri"
        );
        assert_eq!(
            schedule(WRAPPING_WINDOW, false).unwrap().to_string(),
            "2x during 22:00-02:00 UTC"
        );
    }

    #[test_case(&[], false, NO_WINDOWS_ERR ; "no_windows")]
    #[test_case(&[(24, 2)], false, OUTSIDE_DAY_ERR ; "starts_after_the_day")]
    #[test_case(&[(3, 3)], false, EMPTY_WINDOW_ERR ; "empty_window")]
    #[test_case(&[(22, 2)], true, WRAPPING_WEEKDAYS_ERR ; "wrapping_weekdays")]
    fn invalid_schedules_are_rejected(windows: &[(u8, u8)], weekdays_only: bool, error: &str) {
        assert!(
            schedule(windows, weekdays_only)
                .unwrap_err()
                .contains(error)
        );
    }

    /// Each entry bills [`ONE_MILLION`] input tokens, so a row worth
    /// [`LIST_PRICE`] came from the price table and [`RECORDED`] came from the
    /// turn itself. A breakdown nothing can price is not a free session:
    /// `Some(0.0)` would put a confident "$0.000" on screen.
    #[test_case(&[(UNRESOLVABLE, Some(RECORDED)), (ALSO_UNRESOLVABLE, Some(RECORDED))], INPUT_RATE, Some(2.0 * RECORDED) ; "recorded_costs_win_over_the_price_table")]
    #[test_case(&[(CURRENT, None), (UNRESOLVABLE, None)], INPUT_RATE, Some(LIST_PRICE)                                   ; "legacy_counters_use_each_models_own_rates")]
    #[test_case(&[(UNRESOLVABLE, Some(RECORDED)), (CURRENT, None), (ALSO_UNRESOLVABLE, None)], INPUT_RATE, Some(RECORDED + LIST_PRICE) ; "mixed_eras_sum_recorded_and_listed")]
    #[test_case(&[(UNRESOLVABLE, None)], INPUT_RATE, None                                                               ; "a_breakdown_that_prices_to_nothing")]
    #[test_case(&[(CURRENT, None)], UNPRICED, None                                                                      ; "unpriced_model_with_a_breakdown")]
    #[test_case(&[], INPUT_RATE, Some(LIST_PRICE)                                                                       ; "no_breakdown_prices_the_total")]
    #[test_case(&[], UNPRICED, None                                                                                     ; "no_breakdown_on_an_unpriced_model")]
    fn session_cost_bills_every_breakdown(
        entries: &[(&str, Option<f64>)],
        input_rate: f64,
        expected: Option<f64>,
    ) {
        let current = model(CURRENT, input_rate);
        let mut by_model = breakdown(entries);
        let counted = entries.len().max(1) as u32 * ONE_MILLION;

        // With a breakdown the stored running total says nothing about the
        // bill, so the two drifting apart (compaction, a resume) must not
        // change the answer.
        let totals = if entries.is_empty() {
            vec![counted]
        } else {
            vec![counted, 0, 999 * ONE_MILLION]
        };
        for input in totals {
            let total = TokenUsage {
                input,
                ..Default::default()
            };
            assert_eq!(
                settle_session(
                    &total,
                    &mut by_model,
                    &current,
                    false,
                    &ProviderRegistry::default()
                ),
                expected,
                "total of {input} input tokens"
            );
        }
    }

    /// An entry written before turns recorded their cost used to keep only the
    /// cost of the next turn that touched it, so every later load reported the
    /// session as costing whatever it last did.
    #[test_case(&[]                ; "counters_with_no_breakdown")]
    #[test_case(&[(CURRENT, None)] ; "a_breakdown_written_before_costs")]
    fn a_settled_estimate_survives_the_next_turn(entries: &[(&str, Option<f64>)]) {
        let current = model(CURRENT, INPUT_RATE);
        let mut by_model = breakdown(entries);
        let total = TokenUsage {
            input: ONE_MILLION,
            ..Default::default()
        };

        assert_eq!(
            settle_session(
                &total,
                &mut by_model,
                &current,
                false,
                &ProviderRegistry::default()
            ),
            Some(LIST_PRICE)
        );

        *by_model.entry(CURRENT.to_owned()).or_default() += stored(Some(RECORDED));

        assert_eq!(
            settle_session(
                &total,
                &mut by_model,
                &current,
                false,
                &ProviderRegistry::default()
            ),
            Some(LIST_PRICE + RECORDED)
        );
    }

    /// The sibling is priced differently on purpose, so passing here cannot be
    /// `current`'s rates leaking through.
    #[test]
    fn bare_ids_resolve_against_the_current_provider() {
        let current = model(CURRENT, INPUT_RATE);
        let pricing = |input| ModelPricing {
            input,
            ..ModelPricing::ZERO
        };
        let registry = offline_registry(vec![(
            PROVIDER,
            vec![ModelInfo {
                pricing: Some(pricing(SIBLING_INPUT_RATE)),
                ..ModelInfo::id_only(SIBLING.into())
            }],
        )]);
        let usage = stored(None);

        let cost = model_cost(SIBLING, &usage, &current, false, &registry);
        assert_eq!(
            cost,
            model(SIBLING, SIBLING_INPUT_RATE).list_cost(&usage.into(), false)
        );
        assert_ne!(cost, current.list_cost(&usage.into(), false));
    }

    /// What was paid is what was paid, even for an id the table prices
    /// differently, and even for a model that prices to nothing today.
    #[test_case(INPUT_RATE ; "priced_model")]
    #[test_case(UNPRICED   ; "unpriced_model")]
    fn a_recorded_cost_short_circuits_the_price_table(input_rate: f64) {
        let current = model(CURRENT, input_rate);
        assert_eq!(
            model_cost(
                CURRENT,
                &stored(Some(RECORDED)),
                &current,
                false,
                &ProviderRegistry::default()
            ),
            Some(RECORDED)
        );
    }

    #[test_case(true, &[]                => Some(FAST_LIST_PRICE) ; "fast_seeded_from_the_counters")]
    #[test_case(true, &[(CURRENT, None)] => Some(FAST_LIST_PRICE) ; "fast_from_a_breakdown")]
    #[test_case(false, &[(CURRENT, None)] => Some(LIST_PRICE)     ; "standard_from_a_breakdown")]
    fn fast_rates_reach_the_session_total(
        fast: bool,
        entries: &[(&str, Option<f64>)],
    ) -> Option<f64> {
        let mut current = model(CURRENT, INPUT_RATE);
        current.fast_support = Some(FastSupport::Supported);
        current.pricing.fast = Some(FastPricing {
            input: FAST_INPUT_RATE,
            output: 0.0,
        });
        let total = TokenUsage {
            input: ONE_MILLION,
            ..Default::default()
        };

        settle_session(
            &total,
            &mut breakdown(entries),
            &current,
            fast,
            &ProviderRegistry::default(),
        )
    }
}
