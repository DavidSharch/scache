FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o scache cmd/scache/main.go

FROM alpine:latest AS runner
WORKDIR /app
COPY --from=builder /app/scache .
RUN chmod +x scache
CMD ["./scache"]