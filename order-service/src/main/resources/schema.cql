CREATE KEYSPACE IF NOT EXISTS orders
 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};
CREATE TABLE IF NOT EXISTS orders.order_records (
 shard int,
 kind text,
 order_id uuid,
 sku text,
 quantity int,
 status text,
 created_at timestamp,
 PRIMARY KEY ((shard), kind, order_id)
);
