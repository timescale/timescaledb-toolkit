fn main() {
    // 6 years of values
    // 1 value per quarter
    // 1 season = 1 year
    // 4 values per season
    // 24 values total
    //let values = (0..24).map(f64::from).collect::<Vec<_>>();
    let values = [
        362.0,
        385.0,
        432.0,
        341.0,
        382.0,
        409.0,
        498.0,
        387.0,
        473.0,
        513.0,
        582.0,
        474.0,
        544.0,
        582.0,
        681.0,
        557.0,
        628.0,
        707.0,
        773.0,
        592.0,
        627.0,
        725.0,
        854.0,
        661.0,
    ];
    let mut t = forecast::Triple::new(
        forecast::triple::Config {
            smooth: 0.7556,
            trend_smooth: 0.0,
            seasonal_smooth: 0.9837,
            values_per_season: 4,
        },
    );
    for value in &values {
        t.observe(*value);
        //t.minimize();
    }

    eprintln!("| Period |         F | Sales |         S |    b |          I |");
    eprintln!("|--------+-----------+-------+-----------+------+------------|");
    let mut i = 0;
    let mut mse = 0.0;
    for computation in t.computations_iter().unwrap() {
        mse += computation.error * computation.error;
        i += 1;
        let forecast: String = if computation.forecasted_value > 0.0 {
            format!("{:>9.5}", computation.forecasted_value)
        } else {
            "         ".to_owned()
        };
        //         |    Period |    | Sales|       S |  b |       I |
        eprintln!("|     {:>2} | {} |   {} | {:>9.5} | {} | {:>9.8} |",
                  i, forecast, computation.value, computation.smoothed, computation.trend_factor, computation.seasonal_index);
    }
    mse /= f64::from(i);

    for computation in t.forecast_iter(12).unwrap() {
        i += 1;
        //         |    Period |       F | Sales |       S |  b |       I |
        eprintln!("|     {:>2} | {:>9.5} |       | {:>9.5} | {} | {:>9.8} |",
                  i, computation.value, computation.smoothed, computation.trend_factor, computation.seasonal_index);
    }

    eprintln!("MSE is {}", mse);
}
