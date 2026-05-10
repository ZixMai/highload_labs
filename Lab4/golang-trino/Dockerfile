FROM golang:1.25.6-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -a \
    -installsuffix cgo \
    -o trino-lab \
    ./cmd/lab4_trino_script/main.go

FROM busybox:musl AS bb

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/trino-lab /trino-lab
COPY --from=bb /bin/busybox /bin/busybox

ENTRYPOINT ["/trino-lab"]