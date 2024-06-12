FROM golang:1.22.2-alpine as build
ARG DEBUG=0
RUN apk update \
 && apk add gcc libc-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY ./cmd/        ./cmd/
COPY ./pkg/        ./pkg/
COPY ./blueskybot/ ./blueskybot/
RUN CGO_ENABLED=1 GOOS=linux go build $(test $DEBUG -ne 0 && echo -tags sqlite_trace) -o /blueskybot ./cmd/...

FROM build as test
ARG DEBUG=0
RUN CGO_ENABLED=1 GOOS=linux go test $(test $DEBUG -ne 0 && echo -tags sqlite_trace) -v ./...

FROM alpine

RUN apk update \
 && apk add ca-certificates

COPY --from=build /blueskybot /blueskybot
