# Validation — 2026-10-25

本记录区分原生进程、Testcontainers 和完整 Compose 验证，仅列实际执行结果。

## 当前本机 Docker 验证

| 检查 | 实际结果 |
|---|---|
| Windows / WSL / Docker | WSL 3.0.3；Docker Desktop 4.96.3，Engine 29.9.2；Linux containers |
| Java / Maven | Microsoft OpenJDK 21.0.12.1，Maven 3.9.11 |
| `mvn -B verify -Pintegration` | PASS：10 单元测试 + 5 Testcontainers 集成测试，0 failures / errors / skipped |
| Testcontainers | 1.21.4；修复 1.21.3 与 Docker 29 API 最低版本要求不兼容的问题 |
| `docker compose up -d --build` | PASS：7 个容器启动；两个 PostgreSQL 和 Kafka 健康检查通过 |
| `python scripts/smoke.py --grafana-url http://localhost:13000` | PASS：确认、拒绝、幂等、409 冲突、400 校验、JVM metrics、Prometheus targets、Grafana health |
| Dashboard provisioning | PASS：UID microservices-demo，8 个面板 |
| Grafana → Prometheus | PASS：数据源 health=OK，通过 Grafana 代理查询得到两个服务 target 均为 UP |
| Kafka 停机恢复 | PASS：停机时新订单 PENDING、Outbox 失败指标可见；恢复 Kafka 后同一订单 CONFIRMED |
| 原生临时 PostgreSQL | 已关闭，原端口 55432 的临时数据库不再运行 |
| 临时文件 / Docker 数据 | 旧测试文件保留于 D 盘 work/legacy-validation；Docker 虚拟磁盘已由用户迁移到 D 盘 work/docker-storage |

证据：[15 个测试结果](docs/container-test-results.json)、[本机 Grafana 截图](docs/grafana-dashboard.jpg)、[Kafka 恢复结果](docs/broker-recovery.json)、[Grafana API 检查](docs/grafana-verification.json)。

Windows 本机的 3000 端口位于系统保留范围 2993–3092，故仅在被 gitignore 排除的 `.env` 中设置 `GRAFANA_PORT=13000`。Compose 默认仍为 3000，CI 使用默认值。README 记录了端口覆盖方式。

## 远端 CI

[首次 Verify 运行成功](https://github.com/nz284/springboot-kafka-microservices-demo/actions/runs/40203673514)，对应提交 `98a21fb8e27182c225d6a48368080443b9e8894f`。

已从 GitHub Actions jobs 和日志核实：10 单元测试 + 5 Testcontainers 测试通过；Compose 配置、镜像构建/启动、完整 smoke（含监控健康）均成功。该运行不是本机 Docker 验证；后续兼容性补丁提交的 CI 会单独核实并记录。

## 此前原生验证（保留历史证据）

PostgreSQL 17.6 + Kafka 3.9.1 原生进程实际启动两个服务；60 订单、10 并发、每单 2 件：库存 99 → 1，49 CONFIRMED、11 REJECTED，无超卖。非法事件实际进入 DLT；强制 Outbox 插入失败时库存与 reservation 同时回滚；修复后按原 key 重放，订单完成且只扣一次库存。详见 [原生结果](docs/native-results.json)。

截图在 Kafka 故障演示前采集，因此 Outbox publish failures 面板为 No data；该计数器首次失败时才创建。之后的故障与恢复已由指标和数据库业务状态验证，并非伪造截图或用静态图模拟数据。

HTTP 小样本负载只验证脚本与接收路径，不代表端到端异步吞吐量或生产性能结论。完整 Compose 服务保留运行用于本地演示，执行 `docker compose down` 可停止并保留命名卷数据。
