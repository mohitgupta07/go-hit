# Use the official Go image as the base image
FROM golang:1.18-alpine

# Install PostgreSQL client
RUN apk add --no-cache postgresql-client

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN go build -o main .

# Define the entrypoint of the container
CMD ["./main"]
