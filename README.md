# 🏦 Digital Banking System — Fraud Detection Platform

A **Spring Boot microservices-based Digital Banking System** designed to process financial transactions securely using **event-driven architecture, fraud detection, OTP verification, Redis, Apache Kafka, and Saga-based transaction compensation**.

The system simulates a real-world banking transaction workflow where a transaction is validated, processed, analyzed for potential fraud, verified using OTP, and either completed or compensated when a failure or fraud condition occurs.

---

## 📌 Project Overview

The Digital Banking System is designed around multiple independent microservices, with each service responsible for a specific business capability.

### Core Services

* **Account Service** — Manages customer accounts and balances
* **Transaction Service** — Orchestrates money transfers and transaction lifecycle
* **Fraud Detection Service** — Analyzes transactions for suspicious activity
* **Payment Service** — Handles payment-related processing
* **Notification Service** — Handles transaction notifications
* **API Gateway** — Provides a single entry point for client requests

The system uses **Apache Kafka** for asynchronous event-driven communication and **Redis** for temporary OTP storage and verification.

---

# 🏗️ System Architecture

```text
                         ┌───────────────────┐
                         │      Client       │
                         │ Web / Mobile App  │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │    API Gateway    │
                         │ Routing & Security │
                         └─────────┬─────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
       ┌──────────────┐    ┌───────────────┐    ┌──────────────┐
       │   Account    │    │  Transaction  │    │   Payment    │
       │   Service    │    │    Service    │    │   Service    │
       └──────┬───────┘    └───────┬───────┘    └──────────────┘
              │                    │
              │                    │ Kafka
              │                    ▼
              │             ┌───────────────┐
              │             │     Kafka     │
              │             │ Event Broker  │
              │             └───────┬───────┘
              │                     │
              │          ┌──────────┴──────────┐
              │          ▼                     ▼
              │   ┌───────────────┐    ┌────────────────┐
              │   │     Fraud     │    │  Notification  │
              │   │    Detection  │    │    Service     │
              │   │    Service    │    │                │
              │   └───────────────┘    └────────────────┘
              │
              ▼
       ┌──────────────┐
       │    MySQL     │
       │   Database   │
       └──────────────┘

                    ┌──────────────┐
                    │    Redis     │
                    │ OTP / Cache  │
                    └──────────────┘
```

---

# 🔄 Transaction Flow

The system follows a **Saga-style transaction workflow** to maintain consistency across multiple microservices.

### 1. Transaction Initiation

The customer initiates a money transfer through the application.

```text
Client
   ↓
API Gateway
   ↓
Transaction Service
```

The Transaction Service validates the request and starts the transaction workflow.

---

### 2. Deduct Sender Balance

Transaction Service communicates synchronously with Account Service.

```text
Transaction Service
        │
        │ REST
        ▼
Account Service
        │
        ▼
Deduct sender balance
```

The transaction is persisted with:

```text
Status = PROCESSING
```

---

### 3. Publish Transaction Event

After successfully deducting the sender's balance, Transaction Service publishes:

```text
transaction.initiated
```

to Apache Kafka.

```text
Transaction Service
        │
        ▼
Kafka
        │
        ▼
Fraud Detection Service
```

This allows fraud processing to happen asynchronously.

---

### 4. Fraud Detection

The Fraud Detection Service consumes the transaction event and evaluates the transaction for suspicious activity.

```text
transaction.initiated
        ↓
Fraud Detection Service
        ↓
Fraud Analysis
```

The transaction can follow two paths:

```text
                 Fraud Analysis
                       │
              ┌────────┴────────┐
              │                 │
             CLEAN             FRAUD
              │                 │
              ▼                 ▼
          OTP Flow           Compensation
```

---

# 🔐 OTP Verification

For clean transactions, the system performs OTP verification.

Redis is used to temporarily store and retrieve OTP information.

```text
Transaction
     ↓
Generate / Store OTP
     ↓
Redis
     ↓
Customer enters OTP
     ↓
Transaction Service
     ↓
Validate OTP
```

### Correct OTP

```text
Correct OTP
     ↓
Complete Transaction
     ↓
Credit Receiver
     ↓
Publish transaction.completed
```

### Invalid / Expired OTP

```text
Invalid / Expired OTP
        ↓
Compensating Transaction
        ↓
Refund Sender
        ↓
Transaction = FLAGGED
```

---

# 🚨 Fraud Handling

When fraud is detected, the system does not simply discard the transaction.

The system performs a **compensating transaction**.

```text
Sender Balance
      │
      │ Deduct
      ▼
Transaction Processing
      │
      │ Fraud Detected
      ▼
Compensating Transaction
      │
      │ Credit
      ▼
Sender Balance Restored
```

The transaction is then marked as:

```text
FLAGGED
```

and a fraud-related event can be published through Kafka.

---

# 🔁 Saga Pattern

The system uses a **Saga-style distributed transaction pattern** instead of relying on a single distributed database transaction.

### Forward Transaction

```text
1. Deduct sender balance
        ↓
2. Process transaction
        ↓
3. Fraud detection
        ↓
4. OTP verification
        ↓
5. Credit receiver
        ↓
6. Complete transaction
```

### Compensation

If a later step fails:

```text
Deduct sender balance
        ↓
Fraud / OTP failure
        ↓
Credit sender balance
        ↓
Mark transaction FLAGGED
```

This approach helps maintain consistency when operations span multiple independent services.

---

# 📨 Kafka Event-Driven Architecture

Apache Kafka is used for asynchronous communication between services.

### Important Events

| Kafka Topic             | Purpose                                               |
| ----------------------- | ----------------------------------------------------- |
| `transaction.initiated` | Indicates that a transaction has started              |
| `transaction.completed` | Indicates successful transaction completion           |
| `transaction.refunded`  | Indicates that a transaction was compensated/refunded |
| `fraud.detected`        | Indicates suspicious/fraudulent transaction activity  |

### Event Flow

```text
Transaction Service
       │
       ├── transaction.initiated ──► Fraud Detection
       │
       ├── transaction.completed ──► Notification
       │
       ├── transaction.refunded ───► Notification
       │
       └── fraud.detected ─────────► Account / Fraud Workflow
```

Kafka provides loose coupling between services and allows event processing to happen asynchronously.

---

# 🧩 Microservices

## 1. Account Service

Responsible for:

* Creating customer accounts
* Retrieving account information
* Checking account balance
* Deducting account balance
* Crediting account balance
* Blocking accounts

Key APIs include:

```text
POST   /api/v1/accounts
GET    /api/v1/accounts/{accountNumber}
GET    /api/v1/accounts/{accountNumber}/balance
PUT    /api/v1/accounts/{accountNumber}/block
PUT    /api/v1/accounts/{accountNumber}/deduct
PUT    /api/v1/accounts/{accountNumber}/credit
```

---

## 2. Transaction Service

Responsible for transaction orchestration and lifecycle management.

Responsibilities include:

* Initiating transfers
* Calling Account Service
* Maintaining transaction status
* Publishing Kafka events
* OTP verification
* Transaction completion
* Fraud compensation
* Refund processing

Transaction lifecycle:

```text
PROCESSING
    │
    ├──► COMPLETED
    │
    └──► FLAGGED
```

---

## 3. Fraud Detection Service

Responsible for consuming transaction events and analyzing transactions for suspicious activity.

```text
Kafka
  │
  ▼
transaction.initiated
  │
  ▼
Fraud Detection Service
  │
  ├── Clean
  │
  └── Fraud
```

---

## 4. Payment Service

Responsible for payment-related processing within the banking ecosystem.

It is designed as an independent service so payment-related functionality can evolve independently from transaction and account management.

---

## 5. Notification Service

Responsible for processing transaction-related events and handling customer notifications.

Example:

```text
transaction.completed
        ↓
Notification Service
        ↓
Customer Notification
```

and:

```text
transaction.refunded
        ↓
Notification Service
        ↓
Refund Notification
```

---

## 6. API Gateway

Acts as the single entry point for external clients.

Responsibilities include:

* Request routing
* Centralized API access
* Security integration
* Hiding internal service endpoints
* Providing a unified interface to microservices

```text
Client
  ↓
API Gateway
  ↓
Microservices
```

---

# 🛠️ Technology Stack

| Technology          | Usage                                   |
| ------------------- | --------------------------------------- |
| **Java**            | Core application development            |
| **Spring Boot**     | Microservice development                |
| **Spring MVC**      | REST API development                    |
| **Spring Data JPA** | Database persistence                    |
| **MySQL**           | Relational database                     |
| **Apache Kafka**    | Asynchronous event communication        |
| **Redis**           | OTP and temporary data storage          |
| **Zookeeper**       | Kafka coordination in the current setup |
| **Docker**          | Containerization                        |
| **Maven**           | Build and dependency management         |
| **Lombok**          | Boilerplate code reduction              |
| **JWT**             | Authentication                          |
| **RBAC**            | Role-based authorization                |
| **Bean Validation** | Request validation                      |
| **JUnit 5**         | Unit testing                            |
| **Mockito**         | Mock-based testing                      |
| **Postman**         | API testing                             |
| **Git**             | Version control                         |

---

# 🗄️ Data & Storage

The application uses different storage technologies according to the type of data.

### MySQL

Used for persistent business data such as:

```text
Accounts
Transactions
Payments
Fraud-related records
```

### Redis

Used for temporary and fast-access information such as:

```text
OTP
Verification data
Short-lived transaction information
```

This separates long-term transactional data from temporary verification data.

---

# 🐳 Docker

The complete application can be containerized using Docker.

The environment consists of containers for the microservices and supporting infrastructure.

```text
Docker
│
├── API Gateway
├── Account Service
├── Transaction Service
├── Fraud Detection Service
├── Payment Service
├── Notification Service
│
├── MySQL
├── Redis
├── Kafka
└── Zookeeper
```

This makes the application easier to:

* Run locally
* Reproduce across environments
* Deploy independently
* Manage service dependencies

---

# 🔄 Synchronous vs Asynchronous Communication

The system uses both communication patterns.

### Synchronous — REST

Used when an immediate response is required.

Example:

```text
Transaction Service
       │
       │ REST
       ▼
Account Service
```

Used for operations such as:

```text
Deduct balance
Credit balance
Block account
Retrieve balance
```

### Asynchronous — Kafka

Used for event-driven workflows.

```text
Transaction Service
       │
       ▼
Kafka
       │
       ▼
Fraud / Notification Services
```

This reduces direct coupling between services.

---

# 🔒 Security

The platform is designed with security mechanisms suitable for banking applications.

### JWT Authentication

JWT can be used to authenticate API requests.

```text
Client
  ↓
JWT
  ↓
API Gateway
  ↓
Authenticated Request
```

### Role-Based Access Control

RBAC allows access to be controlled according to user roles.

### OTP Verification

OTP provides an additional verification layer before completing sensitive transactions.

### Account Blocking

Suspicious activity can trigger account blocking as part of the fraud workflow.

---

# 📊 Transaction States

A transaction progresses through different states during its lifecycle.

```text
                  ┌─────────────┐
                  │  PROCESSING │
                  └──────┬──────┘
                         │
              ┌──────────┴──────────┐
              │                     │
              ▼                     ▼
        ┌───────────┐          ┌─────────┐
        │ COMPLETED │          │ FLAGGED │
        └───────────┘          └─────────┘
```

### PROCESSING

Transaction has started but has not completed yet.

### COMPLETED

Fraud checks and required verification have succeeded.

### FLAGGED

Transaction has failed due to fraud detection, OTP failure, expiration, or another compensation-triggering condition.

---

# 💡 Key Design Patterns & Concepts

This project demonstrates several backend architecture concepts:

* Microservices Architecture
* RESTful APIs
* Event-Driven Architecture
* Saga Pattern
* Compensating Transactions
* Synchronous Service-to-Service Communication
* Asynchronous Messaging
* Producer-Consumer Pattern
* API Gateway Pattern
* Repository Pattern
* DTO Pattern
* Distributed Transaction Handling
* Temporary Data Storage with Redis

---

# 🎯 Real-World Use Case

The architecture represents a simplified version of how a digital banking platform can process a money transfer.

For example:

```text
Customer
   │
   │ Transfer ₹50,000
   ▼
API Gateway
   │
   ▼
Transaction Service
   │
   ├── Deduct Sender
   │
   ├── Fraud Detection
   │
   ├── OTP Verification
   │
   └── Complete / Compensate
          │
          ├── Success → Credit Receiver
          │
          └── Failure → Refund Sender
```

This architecture allows transaction processing, fraud detection, account management, payment processing and notifications to evolve as independently deployable services.

---

# 📁 Project Structure

```text
digital-banking-system/
│
├── api-gateway/
│
├── account-service/
│
├── transaction-service/
│
├── fraud-detection-service/
│
├── payment-service/
│
├── notification-service/
│
├── docker-compose.yml
│
└── README.md
```

Each microservice is independently buildable and deployable.

---

# 🚀 Getting Started

## Prerequisites

Install:

* Java
* Maven
* Docker
* Docker Compose
* Git

---

## Clone Repository

```bash
git clone https://github.com/dineshkdev21/digital-banking-system
cd digital-banking-system
```

---

## Build the Services

From each service directory:

```bash
mvn clean package
```

---

## Start Infrastructure

Start the required infrastructure using Docker Compose:

```bash
docker-compose up -d
```

This starts the required supporting services such as:

```text
MySQL
Redis
Kafka
Zookeeper
```

along with the configured application services.

---

## Verify Services

Verify that the microservices are running and accessible through the configured ports.

Typical service layout:

```text
API Gateway              → 8080
Account Service          → 8081
Transaction Service      → 8082
Payment Service          → 8083
Fraud Detection Service  → 8084
Notification Service     → 8085
```

---

# 🧪 Testing

The APIs can be tested using tools such as **Postman**.

Example transaction flow:

```text
1. Create Account
2. Check Balance
3. Initiate Transaction
4. Fraud Detection
5. OTP Verification
6. Complete / Compensate Transaction
7. Verify Final Balance
8. Verify Transaction Status
```

---

# 📌 Key Learning Outcomes

This project demonstrates practical implementation of:

* Java backend development
* Spring Boot microservices
* REST API design
* Kafka-based event-driven architecture
* Redis-based OTP verification
* Spring Data JPA
* MySQL persistence
* Saga-style distributed transactions
* Compensating transactions
* Docker containerization
* Inter-service communication
* Transaction state management
* Fraud detection workflow
* Secure transaction processing

---

# 👨‍💻 Author

**Dinesh**

Java Backend Developer

### Tech Focus

```text
Java
Spring Boot
Microservices
REST APIs
Apache Kafka
Redis
MySQL
Spring Data JPA
Docker
Maven
JWT
JUnit
Mockito
```
