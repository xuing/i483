FROM golang:1.24 AS builder
WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o kafak_processor ./kadai2c_kafak_processor.go

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/kafak_processor .
CMD ["./kafak_processor"]