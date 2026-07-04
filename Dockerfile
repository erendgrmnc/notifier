FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/notifier ./cmd/notifier

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=builder /out/notifier /usr/local/bin/notifier
USER nobody
ENTRYPOINT ["notifier"]
