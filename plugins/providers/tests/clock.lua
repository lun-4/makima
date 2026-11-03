local clock = require("providers.clock")
local t = require("maki.test_helpers")

local Y2K_MS = 946684800000
local AUGUST_25_AFTERNOON = 1787665510
local AUGUST_26_MIDNIGHT_MS = 1787702400000

t.case("parse_rfc3339", function()
  t.eq(clock.parse_rfc3339("2000-01-01T00:00:00.000Z"), Y2K_MS)
  t.eq(clock.parse_rfc3339("2000-01-01T00:00:00.964Z"), Y2K_MS + 964)
  t.eq(clock.parse_rfc3339("2026-09-01T00:00:00Z"), 1788220800000)
  t.eq(clock.parse_rfc3339("2000-01-01T01:30:00+01:30"), Y2K_MS)
  t.eq(clock.parse_rfc3339("1999-12-31T23:00:00-01:00"), Y2K_MS)
  t.eq(clock.parse_rfc3339("2000-01-01"), nil)
  t.eq(clock.parse_rfc3339("2000-01-01T00:00:00+0100"), nil)
  t.eq(clock.parse_rfc3339(nil), nil)
end)

t.case("utc_days", function()
  t.eq(clock.utc_date(AUGUST_25_AFTERNOON), "2026-08-25")
  t.eq(clock.utc_date(-1), "1969-12-31")
  t.eq(clock.utc_date(951782400), "2000-02-29")
  t.eq(clock.next_utc_midnight(AUGUST_25_AFTERNOON), AUGUST_26_MIDNIGHT_MS)
end)

t.report()
