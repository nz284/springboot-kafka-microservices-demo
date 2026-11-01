package demo;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import java.time.Instant;
import java.util.*;
import org.springframework.stereotype.Repository;

@Repository
public class CassandraOrderStore {
  // Persistent partition contract: changing this value requires a data migration.
  public static final int SHARDS = 16;
  private final CqlSession session;
  private final PreparedStatement create, find, pending, deleteEvent, complete;

  public CassandraOrderStore(CqlSession session) {
    this.session = session;
    create =
        session.prepare(
            "BEGIN BATCH INSERT INTO order_records"
                + " (shard,kind,order_id,sku,quantity,status,created_at) VALUES"
                + " (?,'ORDER',?,?,?,?,?) IF NOT EXISTS; INSERT INTO order_records"
                + " (shard,kind,order_id,sku,quantity,status,created_at) VALUES"
                + " (?,'EVENT',?,?,?,?,?); APPLY BATCH");
    find =
        session.prepare(
            "SELECT * FROM order_records WHERE shard=? AND kind='ORDER' AND order_id=?");
    pending = session.prepare("SELECT * FROM order_records WHERE shard=? AND kind='EVENT' LIMIT ?");
    deleteEvent =
        session.prepare("DELETE FROM order_records WHERE shard=? AND kind='EVENT' AND order_id=?");
    complete =
        session.prepare(
            "UPDATE order_records SET status=? WHERE shard=? AND kind='ORDER' AND order_id=? IF"
                + " status='PENDING'");
  }

  public static int shard(UUID id) {
    return Math.floorMod(id.hashCode(), SHARDS);
  }

  public boolean insertWithEvent(UUID id, String sku, int quantity) {
    int s = shard(id);
    Instant now = Instant.now();
    // Both rows are in the SAME table and partition. A failed condition writes neither row.
    return session
        .execute(
            create.bind(s, id, sku, quantity, "PENDING", now, s, id, sku, quantity, "PENDING", now))
        .wasApplied();
  }

  public Optional<OrderRecord> find(UUID id) {
    return Optional.ofNullable(session.execute(find.bind(shard(id), id)).one()).map(this::map);
  }

  public List<OrderRecord> pending(int shard, int limit) {
    List<OrderRecord> events = new ArrayList<>();
    session.execute(pending.bind(shard, limit)).forEach(row -> events.add(map(row)));
    return events;
  }

  public void markPublished(UUID id) {
    session.execute(deleteEvent.bind(shard(id), id));
  }

  public boolean complete(UUID id, String status) {
    return session.execute(complete.bind(status, shard(id), id)).wasApplied();
  }

  private OrderRecord map(Row row) {
    return new OrderRecord(
        row.getUuid("order_id"),
        row.getString("sku"),
        row.getInt("quantity"),
        row.getString("status"),
        row.getInstant("created_at"));
  }
}
