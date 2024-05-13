package main

import (
	"math"
	"time"
)

// Channel-based aggregator that reports the global average temperature periodically
//
// Report the averagage temperature across all `k` weatherstations every `averagePeriod`
// seconds by sending a `WeatherReport` struct to the `out` channel. The aggregator should
// terminate upon receiving a singnal on the `quit` channel.
//
// Note! To receive credit, channelAggregator must not use mutexes.
func channelAggregator(
	k int,
	averagePeriod float64,
	getWeatherData func(int, int) WeatherReport,
	out chan WeatherReport,
	quit chan struct{},
) {
	// Your code here.

	batch := 0
	// while loop
	for {
		// set a timer to contrain time for reply, need to convert datatype for second
		timer := time.NewTimer(time.Duration(averagePeriod * float64(time.Second)))

		// create a channel to store reports from stations, limit capacity to k
		observations := make(chan WeatherReport, k)

		// loop to get reports from all k stations
		for id := 0; id < k; id++ {
			// spawn concurrent requests to stations
			go func(id int, batch int) {
				// obs contains the report
				obs := getWeatherData(id, batch)

				// since no default case, need at least one case to pass, otherwise will cause blocking
				select {
				case observations <- obs: // recieve reply
				case <-timer.C: // handle timeout to ignore late reply
				}
			}(id, batch)
		}

		// initialize variable total_temp to store total temperature
		total_temp := 0.0
		// initialize a counter
		num := 0
		// initialize a flag
		collect := true

		for collect {
			select {
			// case one: there is report recieved from channel, gradually build up total temperature
			case obs := <-observations:
				total_temp += obs.Value
				num++

			// case two: timeout, change flag to stop loop, and send report to channel out
			case <-timer.C:
				collect = false
				if num == 0 {
					out <- WeatherReport{Value: math.NaN(), Batch: batch}
				} else {
					out <- WeatherReport{Value: total_temp / float64(num), Batch: batch}
				}

			// case three: recieve from channel quit, then just return
			case <-quit:
				return
			}
		}

		// increment batch for collecting reports from next time periods
		batch++
	}
}
