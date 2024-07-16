
--- Postgres
```yaml
Running tool: /usr/local/go/bin/go test -benchmem -run=^$ -bench ^BenchmarkSaveToDisk$ github.com/Mohitgupta07/go-hit/internal/persistence/dbms

goos: darwin
goarch: arm64
pkg: github.com/Mohitgupta07/go-hit/internal/persistence/dbms
=== RUN   BenchmarkSaveToDisk
BenchmarkSaveToDisk
2024/07/16 05:43:48 Sql Store:: Postgres ready.
2024/07/16 05:43:48 Sql Store:: Postgres ready.
2024/07/16 05:43:48 Sql Store:: Postgres ready.
2024/07/16 05:43:48 Sql Store:: Postgres ready.
BenchmarkSaveToDisk-8               8886            138285 ns/op             462 B/op         14 allocs/op
PASS
ok      github.com/Mohitgupta07/go-hit/internal/persistence/dbms        2.207s

Running tool: /usr/local/go/bin/go test -benchmem -run=^$ -bench ^BenchmarkLoad$ github.com/Mohitgupta07/go-hit/internal/persistence/dbms

goos: darwin
goarch: arm64
pkg: github.com/Mohitgupta07/go-hit/internal/persistence/dbms
=== RUN   BenchmarkLoad
BenchmarkLoad
2024/07/16 05:46:11 Sql Store:: Postgres ready.
saving done
2024/07/16 05:46:11 Sql Store:: Postgres ready.
saving done
2024/07/16 05:46:11 Sql Store:: Postgres ready.
saving done
BenchmarkLoad-8             9631           2622185 ns/op         2046178 B/op      58014 allocs/op
PASS
ok      github.com/Mohitgupta07/go-hit/internal/persistence/dbms        27.077s
```

--- Postgres with IO concurrency = 10
```yaml
Running tool: /usr/local/go/bin/go test -benchmem -run=^$ -bench ^BenchmarkSaveToDisk$ github.com/Mohitgupta07/go-hit/internal/persistence/dbms

goos: darwin
goarch: arm64
pkg: github.com/Mohitgupta07/go-hit/internal/persistence/dbms
=== RUN   BenchmarkSaveToDisk
BenchmarkSaveToDisk
2024/07/16 06:08:07 SQL Store:: PostgreSQL ready.
2024/07/16 06:08:07 SQL Store:: PostgreSQL ready.
2024/07/16 06:08:07 SQL Store:: PostgreSQL ready.
2024/07/16 06:08:07 SQL Store:: PostgreSQL ready.
2024/07/16 06:08:08 SQL Store:: PostgreSQL ready.
BenchmarkSaveToDisk-8              29551             37869 ns/op             493 B/op         15 allocs/op
PASS
ok      github.com/Mohitgupta07/go-hit/internal/persistence/dbms        2.884s

Running tool: /usr/local/go/bin/go test -benchmem -run=^$ -bench ^BenchmarkLoad$ github.com/Mohitgupta07/go-hit/internal/persistence/sfw

goos: darwin
goarch: arm64
pkg: github.com/Mohitgupta07/go-hit/internal/persistence/sfw
=== RUN   BenchmarkLoad
BenchmarkLoad
saving done
saving done
saving done
BenchmarkLoad-8             1450           7427697 ns/op         3173033 B/op      35165 allocs/op
PASS
ok      github.com/Mohitgupta07/go-hit/internal/persistence/sfw 11.359s
```


--- SFW with IO concurrency = 10
```yaml
Running tool: /usr/local/go/bin/go test -benchmem -run=^$ -bench ^BenchmarkSaveToDisk$ github.com/Mohitgupta07/go-hit/internal/persistence/sfw

goos: darwin
goarch: arm64
pkg: github.com/Mohitgupta07/go-hit/internal/persistence/sfw
=== RUN   BenchmarkSaveToDisk
BenchmarkSaveToDisk
BenchmarkSaveToDisk-8              16446            104123 ns/op             782 B/op         17 allocs/op
PASS
ok      github.com/Mohitgupta07/go-hit/internal/persistence/sfw 2.568s

Running tool: /usr/local/go/bin/go test -benchmem -run=^$ -bench ^BenchmarkLoad$ github.com/Mohitgupta07/go-hit/internal/persistence/sfw

goos: darwin
goarch: arm64
pkg: github.com/Mohitgupta07/go-hit/internal/persistence/sfw
=== RUN   BenchmarkLoad
BenchmarkLoad
saving done
saving done
saving done
BenchmarkLoad-8             1704           9426406 ns/op         3909961 B/op      41497 allocs/op
PASS
ok      github.com/Mohitgupta07/go-hit/internal/persistence/sfw 16.302s

```