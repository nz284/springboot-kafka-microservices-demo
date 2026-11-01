# Cassandra migration validation

Date: 2026-11-01 (America/New_York). This record applies to Cassandra-backed Order Service and PostgreSQL-backed Inventory Service.

| Check | Actual result |
|---|---|
| Java 21 build | PASS: `mvn clean verify`, both executable service JARs built |
| Unit tests | PASS: 16 tests, no failures/errors/skips |
| Testcontainers integration | PASS: 9 tests, no failures/errors/skips; real Cassandra 4.1.10, PostgreSQL 17.6 and Kafka 3.9.1 |
| Order atomic creation | PASS: conditional batch creates ORDER and EVENT rows in the same Cassandra partition |
| Concurrent duplicate creation | PASS: 18 concurrent attempts, exactly one applied; replay after dispatch does not recreate EVENT or overwrite the original payload |
| Terminal-state CAS | PASS: competing CONFIRMED/REJECTED updates allow one winner |
| Cassandra relay failure | PASS: a failed Kafka send retains the real Cassandra event row; a later acknowledged send clears it |
| Inventory correctness | PASS: duplicate handling, concurrent no-oversell, SQL rollback, and malformed-record DLT tests retained |
| Runtime dependency separation | PASS: Order JAR contains the Cassandra Java driver and no PostgreSQL, Flyway or spring-jdbc runtime JARs |
| Compose config / build / startup | PASS: seven long-running services and a successful one-shot Cassandra schema init job |
| Full HTTP / monitoring smoke | PASS: confirmation, rejection, replay, 409 conflict, 400 validation, JVM metrics, Prometheus targets and Grafana health |
| Postman collection via Newman 6.2.1 | PASS: 13 HTTP requests (including polling), 23 assertions, zero failures; imported collection and local environment used |
| Real Kafka outage / recovery | PASS: order PENDING and EVENT row retained in Cassandra during outage; failure counter increased; Kafka restart completed the same order, deleted EVENT, and replay did not recreate it or deduct stock twice |
| GitHub CI for this migration | PASS: [Verify run 41344417972](https://github.com/nz284/springboot-kafka-microservices-demo/actions/runs/41344417972), code commit `83071d1a1e9fb4b0cc999876f4f0145c65f7667d`; tests, Compose, full smoke and Newman all succeeded |

Evidence: [all test results](docs/container-test-results.json), [Postman summary](docs/postman-results-summary.json), [Cassandra recovery](docs/cassandra-recovery.json). Full local logs and the Newman execution report are under ignored work/ and outputs/ folders.

The previous Order PostgreSQL container was replaced, while its legacy data volume was preserved. No historical orders were imported into Cassandra. Inventory's existing PostgreSQL volume was preserved. Current validation used new UUIDs and consumed three sample stock units (smoke, Postman, outage/recovery).

Local Grafana uses port 13000 via the ignored `.env` override; CI uses the default 3000. The original PostgreSQL-only runtime screenshots and native reports are explicitly archived under docs/legacy/ and are not current Cassandra evidence.

This is a single-node/RF1 Cassandra demo. Tests do not establish multi-node failover, production capacity, partition retention safety, end-to-end throughput, or automatic recovery of DLT/stuck PENDING business operations. The 16-shard partition contract and LWT tradeoffs are documented in README.md.

The subsequent documentation-only result-recording commit uses `[skip ci]`; it changes no tested application code or collection scripts.
