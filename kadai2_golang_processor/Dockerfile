FROM golang:1.24 AS builder
WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o kafka_processor ./kadai2c_kafka_processor.go

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/kafka_processor .
CMD ["./kafka_processor"]