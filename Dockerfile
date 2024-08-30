FROM rust:1.79-bookworm AS base

WORKDIR /app
COPY . .

FROM base AS builder

ENV RUSTFLAGS='-C target-cpu=native'
RUN cargo build --release

FROM debian:bookworm-slim AS runner

WORKDIR /var/cupid
COPY --from=builder /app/target/release/cupiddb .
EXPOSE 5995

CMD ["/var/cupid/cupiddb"]
