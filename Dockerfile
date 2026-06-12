# Build
FROM rust:1.96-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

# Run
FROM debian:bookworm-slim
RUN useradd --system --uid 10001 pulse \
    && mkdir -p /data && chown pulse /data
COPY --from=builder /app/target/release/pulse /usr/local/bin/pulse
USER pulse
ENV PULSE_PORT=8080 \
    PULSE_DATA_DIR=/data
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["pulse"]
