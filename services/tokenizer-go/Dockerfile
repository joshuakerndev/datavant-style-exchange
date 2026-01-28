FROM golang:1.22-alpine AS build

WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o tokenizer-go ./cmd/tokenizer-go

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/tokenizer-go ./tokenizer-go
EXPOSE 8081
CMD ["/app/tokenizer-go"]
