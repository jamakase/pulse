# Build
FROM rust:1.96-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# Build the heavy dependencies (DataFusion et al.) in their own cached layer:
# only Cargo.toml/lock changes invalidate it, not source edits.
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && touch src/lib.rs \
    && cargo build --release \
    && rm -rf src
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release

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
