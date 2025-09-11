# DeliveryWatch

Real-time delivery analytics system built with Spring Boot, Kafka Streams, and PostgreSQL.

## Architecture

```
order-api (8080) → Kafka → metrics-stream (8081) → PostgreSQL
                     ↓
               Real-time Metrics
```

**Components:**
- **order-api**: REST API for order management with event publishing
- **metrics-stream**: Kafka Streams processor for real-time analytics
- **PostgreSQL**: Metrics storage with time-series data
- **Kafka**: Event streaming backbone

## Features

### Real-time Metrics
- **SLA Tracking**: Percentage of on-time deliveries by city and time window
- **Delivery Time Analytics**: Average and 95th percentile delivery times
- **Alerting**: Automatic alerts when SLA drops below threshold
- **City-based Segmentation**: Metrics grouped by delivery location

### Technical Capabilities
- Event-driven architecture with transactional outbox pattern
- Stream processing with windowed aggregations
- Efficient percentile calculation using reservoir sampling
- REST APIs with proper error handling
- Database migrations with Flyway

## Quick Start

### Prerequisites
- Java 21
- Docker & Docker Compose
- Maven

### 1. Start Infrastructure
```bash
cd docker
docker-compose up -d
```

This starts:
- Kafka (localhost:29092)
- Zookeeper (localhost:2181)
- PostgreSQL (localhost:5433)

### 2. Build & Run Services
```bash
# Build all modules
./mvnw clean compile

# Start metrics-stream (in one terminal)
cd metrics-stream
../mvnw spring-boot:run

# Start order-api (in another terminal)
cd order-api
../mvnw spring-boot:run
```

### 3. Test the System
```bash
# Create an order
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": 12345,
    "city": "Berlin",
    "price": 25.50,
    "promisedAt": "2025-09-11T18:00:00Z"
  }'

# Change order status to delivered
curl -X POST http://localhost:8080/orders/{orderId}/status \
  -H "Content-Type: application/json" \
  -d '{"status": "DELIVERED"}'

# Check metrics
curl "http://localhost:8081/metrics/sla?limit=10"
curl "http://localhost:8081/metrics/delivery-time?limit=10"
curl "http://localhost:8081/metrics/alerts?limit=10"
```

### 4. Generate Test Data
```bash
# Simulate orders for multiple cities
CITY=Berlin ./scripts/simulate_order.sh &
CITY=Paris ./scripts/simulate_order.sh &
CITY=Rome ./scripts/simulate_order.sh &
wait
```

## API Documentation

### Order API (Port 8080)

**Create Order:**
```http
POST /orders
{
  "customerId": 12345,
  "city": "Berlin",
  "price": 25.50,
  "promisedAt": "2025-09-11T18:00:00Z"
}
```

**Update Status:**
```http
POST /orders/{id}/status
{
  "status": "DELIVERED"
}
```

### Metrics API (Port 8081)

**SLA Metrics:**
```http
GET /metrics/sla?city=Berlin&limit=50
```

**Delivery Time Metrics:**
```http
GET /metrics/delivery-time?city=Berlin&limit=50
```

**Recent Alerts:**
```http
GET /metrics/alerts?limit=50
```

## Configuration

### Application Properties

**order-api/application.yml:**
```yaml
server:
  port: 8080
spring:
  datasource:
    url: jdbc:postgresql://localhost:5433/deliverywatch
  kafka:
    bootstrap-servers: localhost:29092
app:
  kafka:
    topic: delivery.order.events.v1
```

**metrics-stream/application.yml:**
```yaml
server:
  port: 8081
app:
  windows:
    size-minutes: 1
  alerts:
    sla-threshold: 90.0
```

## Event Schema

Orders publish events to Kafka topic `delivery.order.events.v1`:

```json
{
  "schema": "delivery.order.events.v1",
  "eventType": "ORDER_CREATED | STATUS_CHANGED",
  "orderId": "uuid",
  "status": "PLACED | PREPARING | DISPATCHED | DELIVERED",
  "customerId": "string",
  "courierId": "string",
  "city": "string",
  "createdAt": "2025-09-11T12:00:00Z",
  "promisedAt": "2025-09-11T13:00:00Z",
  "eventTime": "2025-09-11T12:30:00Z",
  "price": 25.50
}
```

## Database Schema

**SLA Metrics:**
```sql
CREATE TABLE kpi_sla (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    city VARCHAR(64) NOT NULL,
    sla_percent NUMERIC(5,2) NOT NULL,
    PRIMARY KEY (window_start, window_end, city)
);
```

**Delivery Time Metrics:**
```sql
CREATE TABLE kpi_delivery_time (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    city VARCHAR(64) NOT NULL,
    p95_seconds INTEGER NOT NULL,
    avg_seconds INTEGER NOT NULL,
    PRIMARY KEY (window_start, window_end, city)
);
```

## Architecture Decisions

### Event-Driven Design
- **Transactional Events**: Spring Events with `@TransactionalEventListener` ensure events are published only after successful DB commits
- **Retry Logic**: Automatic retries for Kafka publishing failures
- **Async Processing**: Non-blocking event handling

### Stream Processing
- **Real-time**: Metrics update within seconds of order events
- **Windowed Aggregations**: Time-based grouping for consistent reporting
- **State Management**: RocksDB for efficient stream processing state

### Performance Optimizations
- **Reservoir Sampling**: Efficient 95th percentile calculation with bounded memory
- **Caching**: Kafka Streams caching for reduced DB load
- **Indexing**: Optimized database indexes for time-series queries

## Production Considerations

### Monitoring
- Kafka Streams state monitoring
- Database connection health checks
- Custom metrics endpoints

### Scaling
- Horizontal scaling via Kafka partitioning
- Database read replicas for query load
- Separate deployment of order-api and metrics-stream

### Reliability
- Dead letter queues for failed events
- Circuit breakers for external dependencies
- Graceful shutdown with configurable timeouts

## Development

### Testing
```bash
# Run unit tests
./mvnw test

# Run integration tests (requires Testcontainers)
./mvnw verify
```

### Building
```bash
# Build all modules
./mvnw clean package

# Build Docker images
docker build -t deliverywatch/order-api ./order-api
docker build -t deliverywatch/metrics-stream ./metrics-stream
```

## Troubleshooting

**Common Issues:**

1. **Kafka connection failed**: Ensure Kafka is running on port 29092
2. **Database connection error**: Check PostgreSQL on port 5433
3. **No metrics data**: Verify events are being published to Kafka topic
4. **Memory issues**: Adjust JVM heap size for high-volume processing

**Useful Commands:**
```bash
# Check Kafka topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# View Kafka messages
docker exec -it kafka kafka-console-consumer --topic delivery.order.events.v1 --bootstrap-server localhost:9092

# Database access
docker exec -it postgres psql -U postgres -d deliverywatch
```

## Technology Stack

- **Java 21**: Modern LTS version with records and improved performance
- **Spring Boot 3.5**: Latest framework with enhanced observability
- **Kafka Streams**: Stream processing with exactly-once semantics
- **PostgreSQL 16**: Reliable ACID database with time-series optimizations
- **Flyway**: Database migration management
- **Testcontainers**: Integration testing with real dependencies
- **Docker**: Containerization and local development environment
