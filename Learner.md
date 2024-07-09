curl -X POST -H "Content-Type: application/json" -d '{"key": "foo", "value": "bar"}' http://localhost:8080/set

curl 'http://localhost:8080/get?key=foo'

curl -X DELETE 'http://localhost:8080/delete?key=foo'   

go run cmd/go-hit-server/main.go
