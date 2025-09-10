create table users (
    id uuid primary key,
    email varchar(255) not null unique,
    password varchar(255) not null,
    role varchar(32) not null,
    created_at timestamp not null default now()
);

create table orders (
    id uuid primary key,
    customer_id varchar(255) not null,   -- было uuid, теперь строка
    courier_id varchar(255),             -- было uuid, теперь строка, nullable
    city varchar(64) not null,
    status varchar(32) not null,
    price numeric(10,2) not null,
    created_at timestamp not null,
    promised_at timestamp not null,
    updated_at timestamp not null
);

create table order_status_history (
    id bigserial primary key,
    order_id uuid not null,
    status varchar(32) not null,
    event_time timestamp not null,
    constraint fk_hist_order foreign key (order_id) references orders(id)
);

create index idx_order_hist_order_id on order_status_history(order_id);
