package demo;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.*;
import org.springframework.kafka.core.KafkaTemplate;

class CassandraOutboxRelayTest {
  CassandraOrderStore store = mock(CassandraOrderStore.class);

  @SuppressWarnings("unchecked")
  KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

  ObjectMapper json = new ObjectMapper();
  SimpleMeterRegistry meters = new SimpleMeterRegistry();
  OrderRecord row = new OrderRecord(UUID.randomUUID(), "DEMO-SKU", 1, "PENDING", Instant.now());
  CassandraOutboxRelay relay = new CassandraOutboxRelay(store, kafka, json, meters);

  @BeforeEach
  void pending() {
    when(store.pending(anyInt(), anyInt())).thenReturn(List.of());
    when(store.pending(0, 20)).thenReturn(List.of(row));
  }

  @Test
  void publishFailurePreservesDurableEvent() {
    when(kafka.send(eq(Messaging.REQUESTS), anyString(), anyString()))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
    relay.publish();
    verify(store, never()).markPublished(any());
    assertEquals(1, meters.counter("demo.outbox.failures").count());
  }

  @Test
  void acknowledgedPublishDeletesEvent() {
    when(kafka.send(eq(Messaging.REQUESTS), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));
    relay.publish();
    verify(store).markPublished(row.id());
    assertEquals(1, meters.counter("demo.outbox.published").count());
  }

  @Test
  void failedCleanupRemainsRetryable() {
    when(kafka.send(eq(Messaging.REQUESTS), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));
    doThrow(new RuntimeException("Cassandra temporarily unavailable"))
        .when(store)
        .markPublished(row.id());
    relay.publish();
    assertEquals(1, meters.counter("demo.outbox.failures").count());
  }
}
