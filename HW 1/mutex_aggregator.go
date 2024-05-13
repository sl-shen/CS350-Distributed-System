package main

import (
	"math"
	"sync"
	"time"
)

// Mutex-based aggregator that reports the global average temperature periodically
//
// Report the averagage temperature across all `k` weatherstations every `averagePeriod`
// seconds by sending a `WeatherReport` struct to the `out` channel. The aggregator should
// terminate upon receiving a singnal on the `quit` channel.
//
// Note! To receive credit, mutexAggregator must implement a mutex based solution.
func mutexAggregator(
	k int,
	averagePeriod float64,
	getWeatherData func(int, int) WeatherReport,
	out chan WeatherReport,
	quit chan struct{},
) {
	// Your code here.

	// create the mutex
	var mu sync.Mutex
	batch := 0

	for {
		// create a map to store reports from each station
		observations := make(map[int]WeatherReport)

		// set a timer to contrain time for reply, need to convert datatype for second
		timer := time.NewTimer(time.Duration(averagePeriod * float64(time.Second)))

		// loop to get reports from all k stations
		for id := 0; id < k; id++ {
			// spawn concurrent requests to stations
			go func(id int, batch int) {
				// obs contains the report
				obs := getWeatherData(id, batch)

				// use lock
				mu.Lock()
				// unlock after finishing passing the value
				defer mu.Unlock()

				// store report into the map
				// no need to determine the late reply
				observations[id] = obs
			}(id, batch)
		}

		// initialize variable total_temp to store total temperature
		total_temp := 0.0
		// initialize a counter
		num := 0

		select {

		// recieve from channel quit, then just return
		case <-quit:
			return

		// timeout, then calculate the total_temp and num by iterating the map
		case <-timer.C:
			// use lock to prevent data race
			mu.Lock()
			for _, obs := range observations {
				total_temp += obs.Value
				num++
			}
			// unlock
			mu.Unlock()

			if num == 0 {
				out <- WeatherReport{Value: math.NaN(), Batch: batch}
			} else {
				out <- WeatherReport{Value: total_temp / float64(num), Batch: batch}
			}
		}

		// increment batch for collecting reports from next time periods
		batch++

	}
}
