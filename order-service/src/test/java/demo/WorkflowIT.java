package demo;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@SpringBootTest(
    classes = OrderApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkflowIT {
  @Container
  static CassandraContainer<?> cassandra =
      new CassandraContainer<>("cassandra:4.1.10")
          .withEnv("MAX_HEAP_SIZE", "512M")
          .withEnv("HEAP_NEWSIZE", "100M")
          .withInitScript("schema.cql")
          .withStartupTimeout(Duration.ofMinutes(4));

  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry r) {
    r.add(
        "spring.cassandra.contact-points",
        () -> cassandra.getHost() + ":" + cassandra.getMappedPort(9042));
    r.add("spring.cassandra.local-datacenter", () -> "datacenter1");
    r.add("spring.cassandra.keyspace-name", () -> "orders");
    r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    r.add("demo.outbox.initial-delay", () -> "3600000");
  }

  @Autowired CqlSession session;
  @Autowired CassandraOrderStore store;
  @Autowired Orders orders;
  @Autowired KafkaTemplate<String, String> template;
  @Autowired ObjectMapper json;
  @Autowired CassandraOutboxRelay relay;
  @Autowired TestRestTemplate http;

  @BeforeEach
  void reset() {
    session.execute("TRUNCATE order_records");
  }

  @Test
  void createReplayConflictAndConsumeKafkaResult() throws Exception {
    UUID id = UUID.randomUUID();
    orders.create(id, "DEMO-SKU", 2);
    orders.create(id, "DEMO-SKU", 2);
    assertThatThrownBy(() -> orders.create(id, "DEMO-SKU", 3))
        .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
    assertThat(store.pending(CassandraOrderStore.shard(id), 100)).hasSize(1);
    Event e = new Event(id, "DEMO-SKU", 2, "CONFIRMED");
    template
        .send(Messaging.RESULTS, id.toString(), json.writeValueAsString(e))
        .get(10, TimeUnit.SECONDS);
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(orders.get(id).status()).isEqualTo("CONFIRMED"));
    orders.result(json.writeValueAsString(e));
    relay.publish();
    assertThat(store.pending(CassandraOrderStore.shard(id), 100)).isEmpty();
  }

  @Test
  void concurrentCreateAndReplayCannotRecreatePublishedEvent() throws Exception {
    UUID id = UUID.randomUUID();
    int applied = 0;
    try (var pool = Executors.newFixedThreadPool(6)) {
      List<Future<Boolean>> futures = new ArrayList<>();
      for (int i = 0; i < 18; i++)
        futures.add(pool.submit(() -> store.insertWithEvent(id, "DEMO-SKU", 2)));
      for (var f : futures) if (f.get(30, TimeUnit.SECONDS)) applied++;
    }
    assertThat(applied).isEqualTo(1);
    assertThat(store.find(id)).isPresent();
    assertThat(store.pending(CassandraOrderStore.shard(id), 100)).hasSize(1);
    store.markPublished(id);
    assertThat(store.insertWithEvent(id, "DEMO-SKU", 99)).isFalse();
    assertThat(store.pending(CassandraOrderStore.shard(id), 100)).isEmpty();
    assertThat(store.find(id).orElseThrow().quantity()).isEqualTo(2);
  }

  @Test
  void onlyOneTerminalStateCanWin() throws Exception {
    UUID id = UUID.randomUUID();
    store.insertWithEvent(id, "DEMO-SKU", 1);
    try (var pool = Executors.newFixedThreadPool(2)) {
      var a = pool.submit(() -> store.complete(id, "CONFIRMED"));
      var b = pool.submit(() -> store.complete(id, "REJECTED"));
      assertThat((a.get(30, TimeUnit.SECONDS) ? 1 : 0) + (b.get(30, TimeUnit.SECONDS) ? 1 : 0))
          .isEqualTo(1);
    }
    assertThat(store.find(id).orElseThrow().status()).isIn("CONFIRMED", "REJECTED");
  }

  @SuppressWarnings("unchecked")
  @Test
  void outboxSurvivesFailedSendAndCanBeReplayed() {
    UUID id = UUID.randomUUID();
    store.insertWithEvent(id, "DEMO-SKU", 1);
    KafkaTemplate<String, String> broken = mock(KafkaTemplate.class);
    when(broken.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
    new CassandraOutboxRelay(store, broken, json, new SimpleMeterRegistry()).publish();
    assertThat(store.pending(CassandraOrderStore.shard(id), 100)).hasSize(1);
    relay.publish();
    assertThat(store.pending(CassandraOrderStore.shard(id), 100)).isEmpty();
    assertThat(store.find(id)).isPresent();
  }

  @Test
  void httpContractAndValidationArePreserved() {
    String id = UUID.randomUUID().toString();
    Map<String, Object> request = Map.of("orderId", id, "sku", "DEMO-SKU", "quantity", 1);
    var created = http.postForEntity("/api/orders", request, Map.class);
    assertThat(created.getStatusCode().value()).isEqualTo(202);
    assertThat(created.getBody()).containsKeys("id", "sku", "quantity", "status", "created_at");
    assertThat(http.getForEntity("/api/orders/" + id, Map.class).getStatusCode().value())
        .isEqualTo(200);
    assertThat(http.postForEntity("/api/orders", request, Map.class).getStatusCode().value())
        .isEqualTo(202);
    assertThat(
            http.postForEntity(
                    "/api/orders",
                    Map.of("orderId", id, "sku", "DEMO-SKU", "quantity", 2),
                    Map.class)
                .getStatusCode()
                .value())
        .isEqualTo(409);
    assertThat(
            http.postForEntity(
                    "/api/orders",
                    Map.of(
                        "orderId", UUID.randomUUID().toString(), "sku", "DEMO-SKU", "quantity", 0),
                    Map.class)
                .getStatusCode()
                .value())
        .isEqualTo(400);
    assertThat(
            http.getForEntity("/api/orders/" + UUID.randomUUID(), Map.class)
                .getStatusCode()
                .value())
        .isEqualTo(404);
  }
}
