FROM golang:1.22-alpine AS build

WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ingest-api-go ./cmd/ingest-api-go

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/ingest-api-go ./ingest-api-go
EXPOSE 8080
CMD ["/app/ingest-api-go"]
