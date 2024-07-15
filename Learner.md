curl -X POST -H "Content-Type: application/json" -d '{"key": "foo", "value": "bar"}' http://localhost:8080/set

curl 'http://localhost:8080/get?key=foo'

curl -X DELETE 'http://localhost:8080/delete?key=foo'   

go run cmd/go-hit-server/main.go


```pg
# Log in to PostgreSQL as a superuser (like 'postgres')
psql -U postgres

# Inside psql, create a new role (replace 'newuser' and 'password' with your desired username and password)
CREATE ROLE newuser WITH LOGIN PASSWORD 'password';

# Grant necessary privileges (optional)
ALTER ROLE newuser CREATEDB;

# Exit psql
\q



```