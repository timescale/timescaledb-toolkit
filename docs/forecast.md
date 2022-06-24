# [experimental] Forecasting function to predict future value

https://github.com/timescale/product/issues/190

clarkbw proposes "Taking all the data I had and filling the gap into the
future for me" but I'm not sure how to build that:  see questions below.

Two UI proposals in the ticket:
- `forecast(<column>, <start date of the time frame>, <future date>)`
- `time_bucket_forecast( bucket_width, time, start, finish, range)`

The prototype consists of an aggregate (`triple_forecast`) and a function to
generate forecast values (`forecast`) using the Holt-Winters method with
overall, trend, and seasonal smoothing.  I've experimented with other
techinques offering simple smoothing or simple & trend smoothing, but have not
put those into the prototype yet.  It's unclear to me whether we want to offer
multiple methods or just pick one.

Selection of this method is based on reading these:
- Chatfield, C. (2001) Time-Series Forecasting.
- NIST/SEMATECH e-Handbook of Statistical Methods, http://www.itl.nist.gov/div898/handbook/, May 2022.

The formulae refer to previous values.  Computing seasonal index in particular
references values twice as far back as the number of periods per season.
Chatfield has little to say regarding initializing those values, only "the
user must provide" them.  NIST suggests an initialization technique using the
first 2N samples.  That is the approach this prototype takes.

## TODO Big questions

### Choice of Holt-Winters method correct?

I've also tried single- and double- exponential smoothing, and we can offer
those in addition to or in place of triple.

Or something else entirely?

### Holt-Winters smoothing parameters

How to select?  Currently required as inputs but that seems painful.

Handbook suggests "the Marquardt procedure" (presumably
https://en.wikipedia.org/wiki/Levenberg%E2%80%93Marquardt_algorithm) but...?

### UI / API?

Not clear to me how to match what's requested in ticket

### How to integrate with time_bucket?

We require a fixed interval between each value, but real data may need bucketing.

### How to return table?

pgx generates the following:
```SQL ,ignore
CREATE FUNCTION toolkit_experimental."forecast"(
	"agg" toolkit_experimental.TripleForecast,
	"n" integer /* i32 */
) RETURNS TABLE (
	"time" TimestampTz,
	"forecast" double precision,  /* f64 */
	"smoothed" double precision,  /* f64 */
	"trend_factor" double precision,  /* f64 */
	"seasonal_index" double precision  /* f64 */
)
 STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'forecast_triple_wrapper';
```

`RETURNS TABLE` seems right?  But it doesn't work.  Ends up as tuple (see below).

## Aggregate Functions

### triple_forecast

Forecasting with triple exponential smoothing (Holt-Winters method)

```SQL,ignore
toolkit_experimental.triple_forecast(
	-- tuning parameters
	smoothing		DOUBLE PRECISION,
	trend_smoothing		DOUBLE PRECISION,
	seasonal_smoothing	DOUBLE PRECISION,
	-- number of input values to group into season
	values_per_season	INTEGER,
	-- table columns
	"time"			TIMESTAMPTZ,
	value			DOUBLE PRECISION)
```

## Functions

### Test table

Examples below are tested against the following table (values from
https://www.itl.nist.gov/div898/handbook/pmc/section4/pmc436.htm):

```SQL ,non-transactional
SET TIME ZONE 'UTC';
CREATE TABLE forecast_test(time TIMESTAMPTZ, usage DOUBLE PRECISION);
INSERT INTO forecast_test VALUES
	('2020-01-01 00:01:00+00', 362.0),
	('2020-01-01 00:02:00+00', 385.0),
	('2020-01-01 00:03:00+00', 432.0),
	('2020-01-01 00:04:00+00', 341.0),
	('2020-01-01 00:05:00+00', 382.0),
	('2020-01-01 00:06:00+00', 409.0),
	('2020-01-01 00:07:00+00', 498.0),
	('2020-01-01 00:08:00+00', 387.0),
	('2020-01-01 00:09:00+00', 473.0),
	('2020-01-01 00:10:00+00', 513.0),
	('2020-01-01 00:11:00+00', 582.0),
	('2020-01-01 00:12:00+00', 474.0),
	('2020-01-01 00:13:00+00', 544.0),
	('2020-01-01 00:14:00+00', 582.0),
	('2020-01-01 00:15:00+00', 681.0),
	('2020-01-01 00:16:00+00', 557.0),
	('2020-01-01 00:17:00+00', 628.0),
	('2020-01-01 00:18:00+00', 707.0),
	('2020-01-01 00:19:00+00', 773.0),
	('2020-01-01 00:20:00+00', 592.0),
	('2020-01-01 00:21:00+00', 627.0),
	('2020-01-01 00:22:00+00', 725.0),
	('2020-01-01 00:23:00+00', 854.0),
	('2020-01-01 00:24:00+00', 661.0);
```

### forecast

```SQL,ignore
CREATE FUNCTION toolkit_experimental.forecast(
	aggregate	toolkit_experimental.TripleForecast,
	n		INTEGER
) RETURNS TABLE (
	"time"		TIMESTAMPTZ,
	forecast	DOUBLE PRECISION,
	smoothed	DOUBLE PRECISION,
	trend_factor	DOUBLE PRECISION,
	seasonal_index	DOUBLE PRECISION)
```

`aggregate` is returned by `triple_forecast` (above).
`n` is the number of periods ahead to forecast.

#### Example

```SQL
SELECT toolkit_experimental.forecast(
	toolkit_experimental.triple_forecast(
		0.7556,
		0.0,
		0.9837,
		4,
		time,
		usage),
	12)
 FROM forecast_test;
```
TODO - how to return table?
```output
                                        forecast
----------------------------------------------------------------------------------------
 ("2020-01-01 00:25:00+00",739.4964436617983,697.0300150298326,9.75,1.0609248206193074)
 ("2020-01-01 00:26:00+00",710.8884605873315,706.7800150298326,9.75,1.0058129056709753)
 ("2020-01-01 00:27:00+00",690.113146328522,716.5300150298326,9.75,0.9631322231487948)
 ("2020-01-01 00:28:00+00",770.5284946649131,726.2800150298326,9.75,1.0609248206193074)
 ("2020-01-01 00:29:00+00",740.3084880782076,736.0300150298326,9.75,1.0058129056709753)
 ("2020-01-01 00:30:00+00",718.2847638556243,745.7800150298326,9.75,0.9631322231487949)
 ("2020-01-01 00:31:00+00",801.5605456680278,755.5300150298326,9.75,1.0609248206193074)
 ("2020-01-01 00:32:00+00",769.7285155690836,765.2800150298326,9.75,1.0058129056709753)
 ("2020-01-01 00:33:00+00",746.4563813827266,775.0300150298326,9.75,0.9631322231487949)
 ("2020-01-01 00:34:00+00",832.5925966711425,784.7800150298326,9.75,1.0609248206193074)
 ("2020-01-01 00:35:00+00",799.1485430599596,794.5300150298326,9.75,1.0058129056709753)
 ("2020-01-01 00:36:00+00",774.6279989098289,804.2800150298326,9.75,0.9631322231487949)
```

### into_values

```SQL
SELECT toolkit_experimental.into_values(
	toolkit_experimental.triple_forecast(
		0.7556,
		0.0,
		0.9837,
		4,
		time,
		usage))
 FROM forecast_test;
```
TODO - how to return table?
```output
                                                   into_values
-----------------------------------------------------------------------------------------------------------------
 ("2020-01-01 00:01:00+00",362,362,0,9.75,0.9344681038123452,0)
 ("2020-01-01 00:02:00+00",385,382.51218233168277,-2.487817668317234,9.75,1.006355935200835,0)
 ("2020-01-01 00:03:00+00",432,374.7330530408667,-57.26694695913329,9.75,1.1531093262208574,0)
 ("2020-01-01 00:04:00+00",341,375.2711545075024,34.27115450750239,9.75,0.9087948308121643,0)
 ("2020-01-01 00:05:00+00",382,380.91538488629476,-1.0846151137052402,9.75,1.002904580464206,387.4683240165028)
 ("2020-01-01 00:06:00+00",409,363.48477782822727,-45.515222171772734,9.75,1.125673671252293,450.47989874404726)
 ("2020-01-01 00:07:00+00",498,505.2711108580271,7.2711108580271,9.75,0.9843574073863248,339.1938367696195)
 ("2020-01-01 00:08:00+00",387,417.441470065672,30.44147006567198,9.75,0.9283120805564201,516.517031115279)
 ("2020-01-01 00:09:00+00",473,421.9032938865234,-51.096706113476614,9.75,1.1211843836233093,480.87819043648904)
 ("2020-01-01 00:10:00+00",513,499.27864551075317,-13.721354489246835,9.75,1.0267794213277832,424.9011172599055)
 ("2020-01-01 00:11:00+00",582,598.1257404750631,16.12574047506314,9.75,0.9723104900191382,472.5374409769037)
 ("2020-01-01 00:12:00+00",474,468.0077390534324,-5.992260946567626,9.75,1.0145703687421002,681.5407874040964)
 ("2020-01-01 00:13:00+00",544,517.089895374364,-26.910104625635995,9.75,1.0516296726357857,490.5518148401533)
 ("2020-01-01 00:14:00+00",582,581.0423563761865,-0.9576436238135102,9.75,1.0011699438807322,512.2519568330794)
 ("2020-01-01 00:15:00+00",681,651.5635413132003,-29.43645868679971,9.75,1.0446792783168042,599.4004188586018)
 ("2020-01-01 00:16:00+00",557,561.8316905976558,4.831690597655779,9.75,0.9923818521160831,695.456942960813)
 ("2020-01-01 00:17:00+00",628,613.6568559003434,-14.34314409965657,9.75,1.023011318465063,572.2504090989091)
 ("2020-01-01 00:18:00+00",707,663.7225541037956,-43.277445896204426,9.75,1.064869566313243,651.2602243197188)
 ("2020-01-01 00:19:00+00",773,753.1592488180451,-19.840751181954943,9.75,1.0257897951967103,668.3419405908736)
 ("2020-01-01 00:20:00+00",592,623.7084426617818,31.70844266178176,9.75,0.9503651873773881,780.4647965025391)
 ("2020-01-01 00:21:00+00",627,599.7179288669599,-27.282071133040063,9.75,1.0458073673557733,674.5506171147139)
 ("2020-01-01 00:22:00+00",725,682.9912495373219,-42.00875046267811,9.75,1.0609248206193074,625.185981931402)
 ("2020-01-01 00:23:00+00",854,848.2895863876547,-5.710413612345292,9.75,1.0058129056709753,658.3571674205829)
 ("2020-01-01 00:24:00+00",661,687.2800150298326,26.28001502983261,9.75,0.9631322231487948,897.3441209271098)
```
