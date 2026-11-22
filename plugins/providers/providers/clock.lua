-- UTC calendar arithmetic for quota endpoints, which speak RFC 3339 and
-- civil dates while the host speaks epoch milliseconds.

local M = {}

local SECONDS_PER_DAY = 86400
local SECONDS_PER_MINUTE = 60
local MILLIS = 1000
local DAYS_PER_ERA = 146097
local DAYS_TO_EPOCH = 719468
local RFC3339 = "^(%d%d%d%d)-(%d%d)-(%d%d)[Tt ](%d%d):(%d%d):(%d%d)(%.?%d*)(.*)$"

-- Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's
-- days_from_civil).
local function days_from_civil(year, month, day)
  year = year - (month <= 2 and 1 or 0)
  local era = math.floor((year >= 0 and year or year - 399) / 400)
  local year_of_era = year - era * 400
  local day_of_year = math.floor((153 * (month + (month > 2 and -3 or 9)) + 2) / 5) + day - 1
  local day_of_era = year_of_era * 365 + math.floor(year_of_era / 4) - math.floor(year_of_era / 100) + day_of_year
  return era * DAYS_PER_ERA + day_of_era - DAYS_TO_EPOCH
end

local function civil_from_days(days)
  days = days + DAYS_TO_EPOCH
  local era = math.floor((days >= 0 and days or days - DAYS_PER_ERA + 1) / DAYS_PER_ERA)
  local day_of_era = days - era * DAYS_PER_ERA
  local year_of_era = math.floor(
    (day_of_era - math.floor(day_of_era / 1460) + math.floor(day_of_era / 36524) - math.floor(day_of_era / 146096))
      / 365
  )
  local day_of_year = day_of_era - (365 * year_of_era + math.floor(year_of_era / 4) - math.floor(year_of_era / 100))
  local month_index = math.floor((5 * day_of_year + 2) / 153)
  local day = day_of_year - math.floor((153 * month_index + 2) / 5) + 1
  local month = month_index + (month_index < 10 and 3 or -9)
  return year_of_era + era * 400 + (month <= 2 and 1 or 0), month, day
end

-- Epoch milliseconds of an RFC 3339 timestamp (`Z` or `±HH:MM`), or nil.
function M.parse_rfc3339(text)
  if type(text) ~= "string" then
    return nil
  end
  local year, month, day, hour, minute, second, fraction, zone = text:match(RFC3339)
  if not year then
    return nil
  end
  local offset = 0
  if zone ~= "Z" and zone ~= "z" then
    local sign, zone_hours, zone_minutes = zone:match("^([+-])(%d%d):(%d%d)$")
    if not sign then
      return nil
    end
    offset = (tonumber(zone_hours) * 60 + tonumber(zone_minutes)) * SECONDS_PER_MINUTE
    offset = sign == "-" and -offset or offset
  end
  local days = days_from_civil(tonumber(year), tonumber(month), tonumber(day))
  local seconds = days * SECONDS_PER_DAY
    + tonumber(hour) * 3600
    + tonumber(minute) * SECONDS_PER_MINUTE
    + tonumber(second)
    - offset
  return seconds * MILLIS + math.floor((tonumber("0" .. fraction) or 0) * MILLIS)
end

-- `YYYY-MM-DD` of the UTC day containing {secs}.
function M.utc_date(secs)
  return string.format("%04d-%02d-%02d", civil_from_days(math.floor(secs / SECONDS_PER_DAY)))
end

-- Epoch milliseconds of the UTC midnight after {secs}.
function M.next_utc_midnight(secs)
  return (math.floor(secs / SECONDS_PER_DAY) + 1) * SECONDS_PER_DAY * MILLIS
end

return M
