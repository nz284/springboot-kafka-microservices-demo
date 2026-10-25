package demo;

import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

class OutboxTest {
  @SuppressWarnings("unchecked")
  @Test
  void failedPublishDoesNotDeletePendingEvent() {
    JdbcTemplate db = mock(JdbcTemplate.class);
    KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    TransactionTemplate tx = mock(TransactionTemplate.class);
    doAnswer(
            inv -> {
              ((java.util.function.Consumer<org.springframework.transaction.TransactionStatus>)
                      inv.getArgument(0))
                  .accept(mock(org.springframework.transaction.TransactionStatus.class));
              return null;
            })
        .when(tx)
        .executeWithoutResult(any());
    when(db.queryForList(anyString()))
        .thenReturn(
            List.of(
                Map.of(
                    "id",
                    UUID.randomUUID(),
                    "topic",
                    "topic",
                    "event_key",
                    "key",
                    "payload",
                    "{}")));
    when(kafka.send("topic", "key", "{}"))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
    new Outbox(db, new ObjectMapper(), kafka, tx, new SimpleMeterRegistry()).publish();
    verify(db, never()).update(startsWith("delete from outbox"), any(Object.class));
  }

  @Test
  void malformedEventIsRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Outbox.decode(new ObjectMapper(), "[]"));
  }
}
