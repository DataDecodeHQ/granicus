FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /granicus ./cmd/granicus

FROM alpine:3.19

RUN apk add --no-cache ca-certificates python3 py3-pip tzdata

COPY --from=builder /granicus /usr/local/bin/granicus

ENTRYPOINT ["granicus"]
