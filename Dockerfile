FROM --platform=$BUILDPLATFORM golang:1.25.0-alpine AS build

# CI runs on an aarch64 runner while this image ships for linux/amd64. Running
# the Go toolchain through qemu-user is not an option — it dies immediately with
# `fatal error: taggedPointerPack` (and `lfstack.push` on older Go), because the
# runtime's tagged-pointer and preemption assumptions do not hold under
# emulation. So the build stage stays native and cross-compiles instead.
#
# The binary needs cgo for github.com/chai2010/webp, which vendors libwebp's C
# source rather than linking a system library, so all that is required is a C
# cross-compiler — zig provides one for every musl target in a single package,
# and its output links against musl, matching the alpine runtime stage below.
ARG TARGETARCH
RUN apk update && apk add --no-cache git zig

RUN case "$TARGETARCH" in \
      amd64) triple=x86_64-linux-musl ;; \
      arm64) triple=aarch64-linux-musl ;; \
      *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && printf '#!/bin/sh\nexec zig cc -target %s "$@"\n' "$triple" > /usr/local/bin/xcc \
    && chmod +x /usr/local/bin/xcc

ENV CGO_ENABLED=1 GOOS=linux GOARCH=$TARGETARCH CC=/usr/local/bin/xcc

WORKDIR /build

# Copiar apenas arquivos de dependências primeiro para cachear o download
COPY go.mod go.sum ./

# Copiar whatsmeow-lib que é uma dependência local
COPY whatsmeow-lib/ ./whatsmeow-lib/

# Agora fazer download das dependências (com replace funcionando)
RUN go mod download

# Copiar o restante do código
COPY . .

ARG VERSION=dev
RUN go build -ldflags "-X main.version=${VERSION}" -o server ./cmd/evolution-go

FROM alpine:3.19.1 AS final

RUN apk update && apk add --no-cache tzdata ffmpeg libjpeg-turbo libwebp

WORKDIR /app

COPY --from=build /build/server .
COPY --from=build /build/manager/dist ./manager/dist
COPY --from=build /build/VERSION ./VERSION

ENV TZ=America/Sao_Paulo

ENTRYPOINT ["/app/server"]
