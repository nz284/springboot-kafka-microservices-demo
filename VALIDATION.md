# 本次验证记录

验证日期：2026-10-25（America/New_York）。本记录只描述实际执行结果。

| 检查 | 结果 |
|---|---|
| Microsoft OpenJDK 21.0.12.1 + Maven 3.9.11 `mvn -B verify` | PASS，3 个模块、两个可执行 Spring Boot JAR |
| JUnit 5 / Mockito 单元测试 | PASS，10 tests，0 failures，0 errors，0 skipped |
| Docker Compose v2.39.4 `config --quiet` | PASS |
| Python smoke/load 脚本语法检查 | PASS |
| 本地 PostgreSQL 17.6 + Kafka 3.9.1 + 两个服务实际启动 | PASS，Flyway 创建两个独立数据库的表，Kafka 创建 4 个 topic |
| `python scripts/smoke.py --core-only` | PASS：确认、拒绝、请求幂等、409 冲突、400 校验、JVM Prometheus 指标 |
| 60 订单、10 并发、每单 2 件的真实链路 | PASS：初始库存 99，49 CONFIRMED、11 REJECTED，剩余 1，无超卖 |
| 非法 JSON 消息 | PASS，实际投递至 orders.created.v1.DLT |
| 强制库存结果 Outbox 插入违反数据库约束 | PASS，库存扣减及 reservation 标记一起回滚，订单保持 PENDING |
| 临时异常重试耗尽 | PASS，实际进入 DLT；解除故障并按原 key 重放后，原订单 CONFIRMED、库存只扣 1 次 |
| 20 请求、4 并发的 HTTP 接受脚本自测 | PASS，20 个 202、0 HTTP 错误；该小样本不作为性能结论 |
| `mvn -B verify -Pintegration` | BLOCKED：Testcontainers 报 Could not find a valid Docker environment，本机没有 Docker Engine |
| Docker 镜像构建、Compose 全栈容器启动 | NOT RUN：缺少 Docker Engine |
| Prometheus/Grafana 运行与 dashboard 截图 | NOT RUN：仅交付 provisioning、8 面板 dashboard 和演示步骤；未伪造截图 |
| GitHub Actions | 提供 CI 配置，是否通过必须查看实际远端运行 |

本地真实链路使用原生 PostgreSQL/Kafka 进程，没有把它表述成 Testcontainers 或 Docker 测试通过。Testcontainers profile 不会在 Docker 缺失时静默跳过。完整 Compose smoke（不带 `--core-only`）还会检查 Prometheus targets 和 Grafana health，需要在 Docker 环境执行。
