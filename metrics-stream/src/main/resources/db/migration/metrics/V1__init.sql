create table if not exists kpi_sla (
window_start timestamp not null,
window_end timestamp not null,
city varchar(64) not null,
sla_percent numeric(5,2) not null,
primary key (window_start, window_end, city)
);


create table if not exists kpi_delivery_time (
window_start timestamp not null,
window_end timestamp not null,
city varchar(64) not null,
p95_seconds integer not null,
avg_seconds integer not null,
primary key (window_start, window_end, city)
);


create table if not exists alert_anomalies (
id bigserial primary key,
ts timestamp not null,
kind varchar(64) not null,
city varchar(64),
value numeric(10,2),
details text
);

create index if not exists idx_kpi_sla_window
    on kpi_sla (window_end desc, city);

create index if not exists idx_kpi_delivery_time_window
    on kpi_delivery_time (window_end desc, city);

create index if not exists idx_alerts_ts
    on alert_anomalies (ts desc);