# Go-Hit

This is a KV-store tool implemented in Go.

Simple objective: To make a tool which is super scalable, fast, can handle tons of data for scalability.

To start the server:
```shell
go run cmd/go-hit-server/main.go
```

Simple objective of this is to create the best of all worlds like solution that can support multiple backend storage. Also, it should be concurrent in nature.

**Why do we need this?**
- Easy to digest, fast to read and fault tolerant to restart.
- Can support 100k writes a second and data reload (on restart) should be atleast as fast as 100k read per sec. Note that we are just talking about a simple benchmark of write a {"key":"value"} no matter how we store in rdbms, nosql, file storage, etc.

<h1>Benchmark Report:</h1>

| Benchmark Name                      | Operations | Time per Operation | Memory Allocs per Operation | Notes                         |
|-------------------------------------|------------|--------------------|-----------------------------|-------------------------------|
| **Postgres**                        |            |                    |                             |                               |
| BenchmarkSaveToDisk                 | 8886       | 138285 ns/op       | 462 B/op                    |                               |
| BenchmarkLoad                       | 9631       | 2622185 ns/op      | 2046178 B/op                |                               |
| **Postgres with IO concurrency = 10**|            |                    |                             |                               |
| BenchmarkSaveToDisk                 | 29551      | 37869 ns/op        | 493 B/op                    |                               |
| BenchmarkLoad                       | 1450       | 7427697 ns/op      | 3173033 B/op                |                               |
| **SFW with IO concurrency = 10**    |            |                    |                             |                               |
| BenchmarkSaveToDisk                 | 16446      | 104123 ns/op       | 782 B/op                    |                               |
| BenchmarkLoad                       | 1704       | 9426406 ns/op      | 3909961 B/op                |                               |
