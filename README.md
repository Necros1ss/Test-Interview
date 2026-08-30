# Techlab Dev Interview 2026 — RabbitMQ Asynchronous Architecture for E-commerce Microservices

> **Mục tiêu**: Báo cáo kỹ thuật, chứng minh thực nghiệm và hiện thực hóa kiến trúc Microservices hướng sự kiện (EDA) sử dụng **RabbitMQ**, **PostgreSQL**, **API Gateway**, **Transactional Outbox**, và **Saga Choreography** giải quyết triệt để các vấn đề về độ trễ, nghẽn cổ chai và tính toàn vẹn dữ liệu dưới tải cao.

---

## 1. Bài Toán & Bối Cảnh (Problem Statement)

Hệ thống E-commerce Techlab gồm 6 dịch vụ thành phần:
1. **API Gateway**: Expose REST APIs, xử lý Authentication (Bearer Token) và Rate Limiting chống spam/DDoS.
2. **Order Service**: Tiếp nhận đơn hàng, lưu trữ dữ liệu vào database và phản hồi nhanh cho client mà không bị downstream block.
3. **Payment Service**: Xử lý cổng thanh toán, có độ trễ cao (~1.0s) hoặc lỗi ngẫu nhiên, yêu cầu cơ chế Retry tự động.
4. **Inventory Service**: Cập nhật tồn kho, nhạy cảm với Race Condition dưới tải đồng thời cao (High Concurrency).
5. **Notification Service**: Gửi Email/Push bất đồng bộ, không yêu cầu real-time nghiêm ngặt.
6. **Analytics Service**: Thu thập các business events cho báo cáo BI, tuyệt đối không được ảnh hưởng luồng chính.

### Các Vấn Đề Ở Kiến Trúc Synchronous REST (Legacy)
* **Payment Timeout**: Khi Payment Service chậm hoặc lỗi, Order Service bị giữ connection dẫn đến HTTP 504 Timeout và sụt giảm thông lượng toàn hệ thống.
* **Notification Latency**: Việc gửi email (250ms) và push (150ms) cộng dồn trực tiếp vào thời gian phản hồi của API Tạo đơn.
* **Analytics Bottleneck**: Đợt bùng nổ traffic làm quá tải luồng phân tích, gây nghẽn luồng đặt hàng của khách.
* **Tightly Coupled & Duplicated Retry**: Các service phụ thuộc chặt chẽ qua HTTP calls; code retry nằm rải rác và khó kiểm soát.

---

## 2. So Sánh Công Nghệ: RabbitMQ vs. Apache Kafka vs. Apache ActiveMQ

Yêu cầu bài toán đòi hỏi lựa chọn và chứng minh công nghệ Message Queue phù hợp nhất giữa **RabbitMQ**, **Kafka**, và **ActiveMQ**:

### Bảng Ma Trận So Sánh Kỹ Thuật

| Tiêu chí Đánh giá | RabbitMQ | Apache Kafka | Apache ActiveMQ |
| :--- | :--- | :--- | :--- |
| **Mô hình cốt lõi (Core Paradigm)** | **Smart Broker / Dumb Consumer** (Message-oriented) | **Dumb Broker / Smart Consumer** (Distributed Log Streaming) | **JMS Message Broker** (Traditional Broker) |
| **Định tuyến tin nhắn (Routing)** | **Rất mạnh (Flexible Exchanges)**: Topic (`#`, `*`), Direct, Fanout, Headers | Hạn chế (Dựa vào Topic Key partition, cần Stream processor) | Trung bình (JMS Queue & Topic) |
| **Cơ chế Retry & Dead Letter (DLQ)** | **Xuất sắc**: Hỗ trợ native Dead Letter Exchange (DLX) + Message TTL | Phức tạp: Cần tạo riêng retry-topics và quản lý offset commit thủ công | Hỗ trợ qua RedeliveryPlugin & DLQ |
| **Phân phối công việc (Work Distribution)** | **Cân bằng tải tin nhắn linh hoạt**: Hỗ trợ Competing Consumers, Fair dispatching (`prefetch`) | Cố định theo Partition (Số consumer tối đa = Số partitions) | Hỗ trợ Competing Consumers |
| **Độ trễ xử lý (Latency)** | **Cực thấp (Sub-millisecond)** | Thấp (Tối ưu cho batching high-throughput) | Trung bình |
| **Event Replay / Stream Processing** | Hạn chế (Tin nhắn bị xóa sau khi ACK) | **Xuất sắc**: Lưu trữ phân tán, hỗ trợ tua lại (Replay) từ offset | Hạn chế |
| **Độ phức tạp vận hành (Ops Overhead)**| **Trung bình**: Triển khai nhẹ qua Erlang/Docker, UI trực quan | **Cao**: Cần ZooKeeper / KRaft, cấu hình phân vùng và quản trị cluster phức tạp | Trung bình |
| **Độ phù hợp bài toán Techlab** | **RẤT CAO (Tối ưu nhất)** | Có thể dùng nhưng gây Over-engineering | Khả thi nhưng công nghệ cũ hơn |

### Luận Điểm Lựa Chọn RabbitMQ:
1. **Bản chất bài toán**: Hệ thống cần **Transactional Work Queue** (giao việc, xử lý thanh toán, giữ kho, gửi thông báo) với độ trễ phản hồi tức thì (<30ms) và định tuyến linh hoạt theo từng sự kiện.
2. **Khả năng Retry/DLQ tích hợp**: RabbitMQ cung cấp cơ chế DLX và TTL Queues tự nhiên, cho phép thử lại thanh toán 3 lần mà không phải viết thêm hàng trăm dòng code quản lý offset như Kafka.
3. **Độc lập tỷ lệ mở rộng (Independent Scaling)**: Có thể scale số lượng worker tiêu thụ của từng queue độc lập với nhau mà không bị bó buộc bởi số lượng partitions như Kafka.

---

## 3. Kiến Trúc Đề Xuất (Proposed Event-Driven Architecture)

```
[Client / Frontend]
        │
        ▼ (Port 3000 - Bearer Token Auth / Rate Limit / Correlation ID)
┌─────────────────────────────────────────────────────────────────────────┐
│                              API GATEWAY                                │
└──────────┬──────────────────┬──────────────────┬────────────────────────┘
           │                  │                  │
           ▼                  ▼                  ▼
    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
    │Order Service │   │Inventory Svc │   │Payment Svc   │
    │ (Port 3001)  │   │ (Port 3003)  │   │ (Port 3002)  │
    └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
           │                  │                  │
           │ (Transactional   │ (Atomic SQL      │ (Idempotency
           │  Outbox)         │  Lock)           │  Store)
           ▼                  ▼                  ▼
    ┌────────────────────────────────────────────────────┐
    │                POSTGRESQL DATABASE                 │
    │  orders | outbox_events | inventory | payments     │
    └─────────────────────────┬──────────────────────────┘
                              │
                              ▼
    ┌────────────────────────────────────────────────────┐
    │               RABBITMQ MESSAGE BROKER              │
    │                                                    │
    │  • orders.topic:                                   │
    │      - order.created    ──> [inventory.queue]      │
    │      - inventory.reserved ─> [payment.queue]       │
    │      - payment.success  ──> [order.status.queue]   │
    │      - payment.failed   ──> [inventory.queue]      │
    │      - inventory.failed ──> [order.status.queue]   │
    │                                                    │
    │  • notifications.fanout:                           │
    │      ──> [email.notification.queue] (Non-blocking) │
    │      ──> [push.notification.queue]  (Non-blocking) │
    │                                                    │
    │  • payment.dlx:                                    │
    │      ──> [payment.retry.queue] (TTL 3s)            │
    │      ──> [payment.parking.queue] (Exhausted)       │
    └────────────────────────────────────────────────────┘
```

---

## 4. Chu Trình Saga Choreography & Xử Lý Concurrency

### A. Chu Trình Saga Tuần Tự An Toàn:
```
1. Client POST /orders ──> Order Service tạo Order (PENDING) + Outbox Event ──> Trả về 202 Accepted (<30ms)
2. Outbox Worker publish 'order.created' lên 'orders.topic'
3. Inventory Service nhận 'order.created':
     ├── Trường hợp ĐỦ hàng:
     │     ├── Trừ kho nguyên tử (Atomic SQL)
     │     └── Publish 'inventory.reserved'
     │
     └── Trường hợp HẾT hàng:
           ├── Publish 'inventory.failed'
           └── Order Service cập nhật Order = FAILED (Payment KHÔNG bao giờ bị kích hoạt!)

4. Payment Service nhận 'inventory.reserved':
     ├── Trường hợp Thanh toán THÀNH CÔNG:
     │     ├── Ghi nhận Payment SUCCESS
     │     ├── Publish 'payment.success' ──> Order cập nhật = PAID
     │     └── Publish Fanout ──> Gửi Email & Push Notification
     │
     └── Trường hợp Thanh toán THẤT BẠI:
           ├── Retry 3 lần qua DLX + TTL Queue (3s)
           └── Nếu quá 3 lần:
                 ├── Đẩy vào payment.parking.queue
                 ├── Publish 'payment.failed'
                 ├── Inventory Service nhận 'payment.failed' ──> Tự động HOÀN KHO (Release Stock)
                 └── Order Service cập nhật Order = FAILED
```

### B. Chống Race Condition trong Inventory:
Sử dụng câu lệnh SQL trừ kho nguyên tử tại tầng PostgreSQL:
```sql
UPDATE inventory
SET stock = stock - $1, updated_at = CURRENT_TIMESTAMP
WHERE product_id = $2 AND stock >= $1
RETURNING stock;
```
Kết hợp với ràng buộc `UNIQUE (order_id, product_id)` trong bảng `inventory_reservations`, đảm bảo **không bao giờ xảy ra tình trạng Overselling (Bán âm kho)** ngay cả khi scale nhiều pod/instance.

---

## 5. Hướng Dẫn Khởi Chạy Nhanh (Quick Start)

### 1. Khởi chạy toàn bộ hệ thống bằng Docker Compose (Khuyên dùng)
```bash
docker compose up --build -d
```
Hệ thống sẽ bật 8 containers:
* **API Gateway**: `http://localhost:3000` (Auth Header: `Authorization: Bearer techlab-secret-token-2026`)
* **RabbitMQ Management UI**: `http://localhost:15672` (`guest` / `guest`)
* **PostgreSQL Database**: `localhost:5432` (`postgres` / `postgres` - Database: `techlab_db`)
* **5 Backend Services**: Order, Payment, Inventory, Notification, Analytics.

---

### 2. Chạy Toàn Bộ 23 Automated Tests
```bash
npm test
```
Bộ test bao gồm:
* `tests/gateway.test.js`: Kiểm thử Token Auth, Rate Limiting, Correlation ID Injection.
* `tests/inventory-concurrency.test.js`: Kiểm thử Race Condition đa luồng (20 request tranh 5 sản phẩm).
* `tests/order-saga.test.js`: Kiểm thử luồng Async và chuyển đổi trạng thái Saga (`PAID` vs `FAILED`).
* `tests/outbox.test.js`: Kiểm thử Transactional Outbox.
* `tests/payment-dlq.test.js`: Kiểm thử Dead Letter Exchange (DLX) và Idempotency.

---

### 3. Chạy Toàn Bộ 6 Microservices và Dual-Metric Benchmark
```bash
node runner.js
```

---

## 6. Kết Quả Đo Lường Thực Nghiệm (Dual-Metric Benchmark)

Chạy thực nghiệm so sánh với 50 requests tải đồng thời:

```
========================================================================
       DUAL-METRIC BENCHMARK COMPARISON (SYNC vs ASYNC)
========================================================================
┌─────────┬────────────────────────────────┬───────────────────────────────┬───────────────────────────────┐
│ (index) │ Metric                         │ Sync REST (Legacy)            │ Async RabbitMQ (Proposed)     │
├─────────┼────────────────────────────────┼───────────────────────────────┼───────────────────────────────┤
│ 0       │ 'API Acceptance Success Rate'  │ '41/50' (18% Timeout/Error)   │ '50/50' (0% Error)            │
│ 1       │ 'API Acceptance Error Rate'    │ '18.00%'                      │ '0.00%'                       │
│ 2       │ 'API Throughput (RPS)'         │ '10.90 req/sec'               │ '1250.00 req/sec'             │
│ 3       │ 'API Average Latency'          │ '1721.49 ms'                  │ '15.46 ms'                    │
│ 4       │ 'API p95 Latency'              │ '1986 ms'                     │ '27 ms'                       │
│ 5       │ 'Downstream Timeout Blocking'  │ 'Blocks Order Service (504s)' │ 'Non-blocking (202 Accepted)' │
│ 6       │ 'Saga State Consistency'       │ 'Partial Failure Risk'        │ 'Safe Compensation & DLQ'     │
└─────────┴────────────────────────────────┴───────────────────────────────┴───────────────────────────────┘

🚀 PROOF: Async RabbitMQ reduced API p95 Latency by 98.6% while isolating downstream latency and ensuring Saga consistency!
```

---

## 7. Triển Khai Kubernetes Manifests (`k8s/`)

Thư mục `k8s/` cung cấp đầy đủ manifests sẵn sàng deploy:
* `k8s/namespace.yaml`
* `k8s/configmap-secrets.yaml`
* `k8s/rabbitmq.yaml`
* `k8s/postgres.yaml`
* `k8s/api-gateway.yaml`
* `k8s/order-service.yaml`
* `k8s/payment-service.yaml`
* `k8s/inventory-service.yaml`
* `k8s/notification-service.yaml`
* `k8s/analytics-service.yaml`

Các pods được cấu hình đầy đủ `readinessProbe`, `livenessProbe`, `resources` requests/limits và xử lý Graceful Shutdown khi nhận tín hiệu `SIGTERM`.
