FROM golang:1.23 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /vk-gpu-provider ./cmd/vk-gpu-provider/

FROM registry.access.redhat.com/ubi9-micro:latest
COPY --from=builder /vk-gpu-provider /usr/local/bin/
ENTRYPOINT ["vk-gpu-provider"]
