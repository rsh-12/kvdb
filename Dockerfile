FROM golang:1.23.6-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod tidy
COPY . .
RUN GOOS=linux GOARCH=amd64 go build -o kvdb ./cmd/app/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/kvdb .
COPY go.mod .

EXPOSE 8080

ENV CONFIG_PATH=./config/docker.yaml

CMD ["./kvdb"]