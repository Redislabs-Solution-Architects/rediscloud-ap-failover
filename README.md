# Redis Active-Passive Failover Demo

A self-contained demo app that showcases Redis Cloud Active-Passive replication and automated failover orchestration. Built for Redis Solution Architects.

## What It Does

1. **Step 1** — Enter your Redis Cloud API credentials (Account Key + User API Key)
2. **Step 2** — Connect two Redis Cloud databases (Source & Target) with their Subscription & Database IDs
3. **Step 3** — Interactive dashboard:
   - Enable/disable Active-Passive replication between databases (via Cloud API)
   - Generate live SET/GET traffic with configurable rates
   - View real-time key counts (DBSIZE) and recent keys from both databases
   - Execute a 4-step automated failover: Pause Traffic → Disable Replication → Switch Writes → Resume Traffic
   - Flush either database

## Prerequisites

- Two **Redis Cloud** databases (any tier)
- **Redis Cloud API credentials** — generate at [Redis Cloud Console → Account → API Keys](https://app.redislabs.com/#/account/api-keys)
- The **Subscription ID** and **Database ID** for each database (visible in the Cloud Console)

## Build & Run Locally (Docker)

```bash
# Clone the repo
git clone <repo-url>
cd WorkspotPOC

# Build the image
docker build -t workspot-poc .

# Run it
docker run -p 8080:8080 workspot-poc
```

Open **http://localhost:8080**.

## Build & Run Without Docker

Requires **Java 17+** and **Maven 3.8+**.

```bash
mvn spring-boot:run
```

Or build a JAR and run it:

```bash
mvn package -DskipTests
java -jar target/workspot-poc-1.0-SNAPSHOT.jar
```

## Architecture

| Component | Tech |
|-----------|------|
| Backend | Spring Boot 3.2 (Java 17) |
| Frontend | Single-page HTML/JS/CSS |
| Redis client | Jedis |
| Cloud API | REST via `java.net.HttpClient` |

All state is in-memory — no external database required beyond the two Redis instances you connect.
