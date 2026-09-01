# Techlab Dev Interview 2026 — RabbitMQ Asynchronous Architecture for E-commerce Microservices

Dự án hiện thực hóa kiến trúc Microservices hướng sự kiện (**Event-Driven Architecture**) sử dụng **RabbitMQ**, **Node.js**, **PostgreSQL** và **Kubernetes**, giải quyết triệt để bài toán về độ trễ cao, nghẽn cổ chai downstream và bảo vệ toàn vẹn dữ liệu kho hàng dưới tải lớn cho sàn thương mại điện tử Techlab.

---

## 1. Sơ Lược Hướng Giải Quyết (Solution Overview)

Sau khi phân tích bài toán (500k người dùng, 50k DAU, 3k–5k đơn/ngày, tăng trưởng 3–5 lần), chúng tôi lựa chọn **RabbitMQ** làm Message Broker trung tâm kết hợp với các mẫu thiết kế doanh nghiệp:

1. **Tách rời luồng xử lý (Decoupling & Asynchronous Processing):** API Gateway tiếp nhận request và Order Service phản hồi ngay `HTTP 202 Accepted` trong **< 30ms**, không bị giữ kết nối bởi Payment hay downstream services.
2. **Saga Choreography Pattern:** Điều phối giao dịch phân tán giữa Order, Inventory và Payment; tự động kích hoạt **Compensating Transaction (Hoàn kho)** nếu thanh toán thất bại sau tối đa 3 lần thử lại.
3. **Transactional Outbox Pattern:** Đồng bộ trạng thái đơn hàng và sự kiện Outbox trong cùng một SQL Transaction nguyên tử, giải quyết triệt để rủi ro *Dual-Write*.
4. **Atomic Inventory Reservation:** Trừ tồn kho nguyên tử tại tầng Database (`WHERE stock >= qty`), chống hoàn toàn tình trạng **Overselling (Bán âm kho)**.
5. **Dead Letter Exchange (DLX) & Exponential Retry:** Tự động thử lại thanh toán qua hàng đợi có độ trễ (TTL 3s) và chuyển vào Parking Queue an toàn khi vượt ngưỡng.

### Sơ Đồ Kiến Trúc Hệ Thống

```
                     [ CLIENT / FRONTEND ]
                               │
                               ▼ (Port 3000: Bearer Token Auth / Rate Limiting / Trace ID)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                      API GATEWAY                                       │
└───────────────┬───────────────────────────────┬───────────────────────────────┬────────┘
                │                               │                               │
                ▼                               ▼                               ▼
     ┌────────────────────┐          ┌────────────────────┐          ┌────────────────────┐
     │   Order Service    │          │ Inventory Service  │          │  Payment Service   │
     │    (Port 3001)     │          │    (Port 3003)     │          │    (Port 3002)     │
     └──────────┬─────────┘          └──────────┬─────────┘          └──────────┬─────────┘
                │                               │                               │
                │ (Transactional                │ (Atomic SQL                   │ (Idempotency
                │  Outbox Pattern)              │  Stock Lock)                  │  Key Store)
                ▼                               ▼                               ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                  POSTGRESQL DATABASE                                   │
│                  orders | outbox_events | inventory | payments | reservations          │
└───────────────────────────────────────────────┬────────────────────────────────────────┘
                                                │
                                                ▼ (Transactional Outbox Dispatcher)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                RABBITMQ MESSAGE BROKER                                 │
│                                                                                        │
│  • orders.topic (Topic Exchange)                                                       │
│       ├── order.created        ───► [ inventory.queue ]                                │
│       ├── inventory.reserved   ───► [ payment.queue ]                                  │
│       ├── payment.success      ───► [ order.status.queue ]                             │
│       ├── payment.failed       ───► [ inventory.queue ] (Saga Compensation Release)    │
│       ├── inventory.failed     ───► [ order.status.queue ] (Mark Order FAILED)         │
│       └── order.#              ───► [ analytics.queue ] (BI Event Ingestion)           │
│                                                                                        │
│  • notifications.fanout (Fanout Exchange)                                              │
│       ├── (Broadcast)          ───► [ email.notification.queue ]                       │
│       └── (Broadcast)          ───► [ push.notification.queue ]                        │
│                                                                                        │
│  • payment.dlx (Dead Letter Exchange)                                                  │
│       ├── payment.retry        ───► [ payment.retry.queue ] (TTL 3s Retry)             │
│       └── (Max 3 Retries)      ───► [ payment.parking.queue ] (Permanent Failure)      │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Mục Lục Tài Liệu Chi Tiết (Documentation)

Để xem phân tích kỹ thuật chuyên sâu và hướng dẫn cài đặt từng môi trường, vui lòng tham khảo các tài liệu sau:

* 📄 **[SOLUTION_REPORT.md](./SOLUTION_REPORT.md)**: **Báo cáo kỹ thuật chi tiết & Thực nghiệm đối chứng**
  * Ma trận so sánh lý thuyết: **RabbitMQ** vs **Kafka** vs **ActiveMQ**.
  * Mermaid Sequence Diagram chu trình Saga, Compensating Transaction & DLQ.
  * Bảng số liệu đo đạc thực nghiệm (Giảm 98.6% p95 Latency, Throughput tăng 114 lần, 0% lỗi).
  * Đánh giá Trade-offs & chuẩn bị cho môi trường Production.
* 🛠️ **[SETUP_GUIDE.md](./SETUP_GUIDE.md)**: **Sổ tay hướng dẫn cài đặt & khởi chạy từ A–Z**
  * Hướng dẫn chi tiết từng bước cho máy tính mới (Windows, macOS, Linux).
  * 3 phương thức khởi chạy: Docker Compose, In-Memory Local Mode, và Kubernetes.
  * Hướng dẫn gọi thử nghiệm API (cURL, Postman) và khắc phục sự cố (Troubleshooting).

---

## 3. Hướng Dẫn Cài Đặt & Khởi Chạy Nhanh (Quick Start)

### 1. Yêu Cầu Cài Đặt
* **Node.js**: v18.x hoặc v20.x+
* **Docker & Docker Compose** (hoặc chạy qua In-Memory mode không cần cài DB)

### 2. Thiết Lập Môi Trường
```bash
# 1. Cài đặt dependencies
npm install

# 2. Tạo file cấu hình môi trường từ file mẫu
cp .env.example .env
```

### 3. Khởi Chạy Bằng Docker Compose (Khuyên dùng 🌟)
```bash
docker compose up --build -d
```
Hệ thống sẽ bật 8 containers:
* **API Gateway**: `http://localhost:3000` (Header: `Authorization: Bearer techlab-secret-token-2026`)
* **RabbitMQ Management UI**: `http://localhost:15672` (`guest` / `guest`)
* **PostgreSQL Database**: `localhost:5432` (`postgres` / `postgres`)
* **5 Backend Services**: Order (3001), Payment (3002), Inventory (3003), Notification (3004), Analytics (3005).

*(Nếu máy chưa cài Docker, chỉ cần chạy `node runner.js` để tự động bật 6 services bằng In-Memory Store).*

---

## 4. Chạy Kiểm Thử & Đo Đạc Hiệu Năng (Testing & Benchmark)

### 1. Chạy 25 Automated Tests
Bộ test độc lập kiểm thử toàn diện Gateway, Saga, Outbox, Concurrency và DLQ:
```bash
npm test
```
Hoặc xem chi tiết từng bước kiểm thử:
```bash
npm run test:verbose
```

### 2. Chạy Benchmark Đo Đạc Đối Chứng (Sync REST vs Async RabbitMQ)
```bash
npm run benchmark:all
```

---

## 5. Cấu Trúc Thư Mục Dự Án (Project Structure)

```
Test-Interview/
├── benchmark/               # Công cụ đo đạc đối chứng latency, RPS và error rate
│   └── load-test.js
├── common/                  # Các module dùng chung (Database, Outbox, RabbitMQ Topology)
│   ├── db.js
│   ├── outbox.js
│   └── rabbitmq.js
├── k8s/                     # Kubernetes Manifests (Deployments, Services, ConfigMaps)
│   ├── api-gateway.yaml
│   ├── order-service.yaml
│   ├── payment-service.yaml
│   ├── inventory-service.yaml
│   ├── notification-service.yaml
│   ├── analytics-service.yaml
│   ├── rabbitmq.yaml
│   └── postgres.yaml
├── services/                # Mã nguồn 6 Microservices độc lập
│   ├── api-gateway/
│   ├── order-service/
│   ├── payment-service/
│   ├── inventory-service/
│   ├── notification-service/
│   └── analytics-service/
├── tests/                   # 25 Automated Tests
│   ├── gateway.test.js
│   ├── inventory-concurrency.test.js
│   ├── order-saga.test.js
│   ├── outbox.test.js
│   └── payment-dlq.test.js
├── docker-compose.yml       # Docker Compose Stack 8 containers
├── runner.js                # Script khởi chạy đa tiến trình cho local dev
├── README.md                # Tài liệu tổng quan & hướng dẫn nhanh
├── SETUP_GUIDE.md           # Hướng dẫn chi tiết setup & troubleshooting
└── SOLUTION_REPORT.md       # Báo cáo kỹ thuật chi tiết & dữ liệu benchmark
```
