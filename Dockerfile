FROM golang:1.25.0-alpine AS build

# CI builds this image for linux/amd64 on an aarch64 runner, so the Go
# toolchain runs under qemu-user. Go's signal-based async preemption is what
# makes it crash there with errors like `fatal error: lfstack.push`; disabling
# preemption is the standard workaround and costs nothing on a native build.
# The binary itself needs cgo (libjpeg-turbo, libwebp), so this stage cannot be
# cross-compiled the way a pure-Go one would be.
ENV GODEBUG=asyncpreemptoff=1

RUN apk update && apk add --no-cache git build-base libjpeg-turbo-dev libwebp-dev

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
RUN CGO_ENABLED=1 go build -ldflags "-X main.version=${VERSION}" -o server ./cmd/evolution-go

FROM alpine:3.19.1 AS final

RUN apk update && apk add --no-cache tzdata ffmpeg libjpeg-turbo libwebp

WORKDIR /app

COPY --from=build /build/server .
COPY --from=build /build/manager/dist ./manager/dist
COPY --from=build /build/VERSION ./VERSION

ENV TZ=America/Sao_Paulo

ENTRYPOINT ["/app/server"]
