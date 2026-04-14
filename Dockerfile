# --- Build Stage ---
FROM golang:1.26-bookworm AS builder

WORKDIR /src

# Copy source code.
COPY . .

RUN go mod download

# Build the binary.
RUN CGO_ENABLED=0 GOOS=linux go build -o kubernetes-impulse .

# --- Final Stage ---
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /src/kubernetes-impulse /kubernetes-impulse

ENTRYPOINT ["/kubernetes-impulse"]
