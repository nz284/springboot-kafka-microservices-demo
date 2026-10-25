create table outbox(id uuid primary key,topic varchar(100) not null,event_key varchar(100) not null,payload text not null,created_at timestamptz not null default now());
create table stock(sku varchar(64) primary key,available integer not null check(available>=0));
insert into stock values('DEMO-SKU',100);
create table reservations(order_id uuid primary key,sku varchar(64) not null,quantity integer not null,status varchar(20) not null);
