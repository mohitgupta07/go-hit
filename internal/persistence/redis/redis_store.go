package redis

// Imagine we want to store the same thing in redis as well.
// In our case redis may act like a L2 cache where L1 is our main go-hit and then it would hit on redis.
// This may/may not give a good benchmarking option unless we disable the L2 cache funda and simply execute load test on redis.
