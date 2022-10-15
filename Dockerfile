FROM registry.gitlab.com/gitlab-org/gitlab-build-images:golangci-lint-alpine as builder
WORKDIR /build
COPY . /build/
RUN echo $(ls -1 /build/)
RUN CGO_ENABLED=0 GOOS=linux go build -a -o mailsender ./cmd/mailsender


# generate clean, final image for end users
FROM quay.io/jitesoft/alpine:3.11.3
COPY --from=builder /build/mailsender .
# executable

ENTRYPOINT [ "./mailsender" ]
