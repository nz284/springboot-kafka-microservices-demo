package demo;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@SpringBootTest(
    classes = OrderApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkflowIT {
  @Container
  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.6-alpine");

  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry r) {
    r.add("spring.datasource.url", postgres::getJdbcUrl);
    r.add("spring.datasource.username", postgres::getUsername);
    r.add("spring.datasource.password", postgres::getPassword);
    r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    r.add("demo.outbox.delay", () -> "100");
  }

  @Autowired JdbcTemplate db;
  @Autowired KafkaTemplate<String, String> template;
  @Autowired ObjectMapper json;
  @Autowired Orders orders;

  @Test
  void createReplayConflictAndConsumeResult() throws Exception {
    UUID id = UUID.randomUUID();
    orders.create(id, "DEMO-SKU", 2);
    orders.create(id, "DEMO-SKU", 2);
    assertThatThrownBy(() -> orders.create(id, "DEMO-SKU", 3))
        .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
    assertThat(db.queryForObject("select count(*) from orders where id=?", Integer.class, id))
        .isEqualTo(1);
    Event e = new Event(id, "DEMO-SKU", 2, "CONFIRMED");
    template
        .send(Messaging.RESULTS, id.toString(), json.writeValueAsString(e))
        .get(10, TimeUnit.SECONDS);
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(orders.get(id).get("status")).isEqualTo("CONFIRMED"));
    orders.result(json.writeValueAsString(e));
    assertThat(orders.get(id).get("status")).isEqualTo("CONFIRMED");
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(db.queryForObject("select count(*) from outbox", Integer.class))
                    .isZero());
  }
}
