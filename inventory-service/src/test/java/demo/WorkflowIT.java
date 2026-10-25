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
    classes = InventoryApplication.class,
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
  @Autowired Inventory inventory;

  @BeforeEach
  void reset() {
    db.update("update stock set available=100 where sku='DEMO-SKU'");
  }

  @Test
  void kafkaDeliveryReservesExactlyOnce() throws Exception {
    UUID id = UUID.randomUUID();
    String payload = json.writeValueAsString(new Event(id, "DEMO-SKU", 3, "PENDING"));
    template.send(Messaging.REQUESTS, id.toString(), payload).get(10, TimeUnit.SECONDS);
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        db.queryForObject(
                            "select count(*) from reservations where order_id=?",
                            Integer.class,
                            id))
                    .isEqualTo(1));
    inventory.reserve(payload);
    assertThat(db.queryForObject("select available from stock where sku='DEMO-SKU'", Integer.class))
        .isEqualTo(97);
  }

  @Test
  void concurrentReservationsNeverOversell() throws Exception {
    try (var pool = Executors.newFixedThreadPool(10)) {
      var futures = new ArrayList<Future<?>>();
      for (int i = 0; i < 20; i++) {
        String payload =
            json.writeValueAsString(new Event(UUID.randomUUID(), "DEMO-SKU", 10, "PENDING"));
        futures.add(pool.submit(() -> inventory.reserve(payload)));
      }
      for (var f : futures) f.get(30, TimeUnit.SECONDS);
    }
    assertThat(db.queryForObject("select available from stock where sku='DEMO-SKU'", Integer.class))
        .isZero();
  }

  @Test
  void failureRollsBackReservationMarker() {
    UUID id = UUID.randomUUID();
    db.execute("alter table stock add constraint test_rollback check(available=100)");
    try {
      assertThatThrownBy(
              () ->
                  inventory.reserve(
                      json.writeValueAsString(new Event(id, "DEMO-SKU", 1, "PENDING"))))
          .isInstanceOf(Exception.class);
      assertThat(
              db.queryForObject(
                  "select count(*) from reservations where order_id=?", Integer.class, id))
          .isZero();
    } finally {
      db.execute("alter table stock drop constraint test_rollback");
    }
  }

  @Test
  void malformedRecordReachesDlt() throws Exception {
    var props = new Properties();
    props.put("bootstrap.servers", kafka.getBootstrapServers());
    props.put("group.id", UUID.randomUUID().toString());
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    try (var consumer =
        new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props)) {
      consumer.subscribe(List.of(Messaging.REQUESTS + ".DLT"));
      template.send(Messaging.REQUESTS, "bad", "{broken").get(10, TimeUnit.SECONDS);
      long end = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      boolean found = false;
      while (System.nanoTime() < end && !found)
        for (var record : consumer.poll(Duration.ofMillis(500)))
          if ("{broken".equals(record.value())) found = true;
      assertThat(found).isTrue();
    }
  }
}
