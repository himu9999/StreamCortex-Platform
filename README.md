# StreamCortex: AI-Native OTT Backend Pipeline

## 📌 Overview
A highly scalable, event-driven backend simulation designed for OTT platforms. It decouples high-throughput video telemetry ingestion from heavy AI processing using RabbitMQ and Node.js.

## 🏗 Architecture Decisions (ADRs)

RabbitMQ over Direct DB Inserts: Video watch events occur at massive scale. Writing them directly to a database creates bottlenecks. By pushing events to a RabbitMQ video_events exchange, the API responds in milliseconds (202 Accepted), allowing background workers to process the data asynchronously.

Batch Processing for LLMs: Passing single events to an AI API is cost-prohibitive and hits rate limits quickly. The consumer uses an in-memory Map to batch events by userId (acting as a micro-aggregator) before triggering the AI persona analysis.

Defensive Consumer Design: Implemented explicit try/catch blocks around message parsing with channel.nack routing for malformed payloads, ensuring the consumer never crashes on bad data.

## 🤖 AI-Native Workflow Documentation

Tool Used: Cursor / Claude

Engineering Override Example: When generating the initial RabbitMQ publisher, the AI suggested creating a new connection for every HTTP request. I recognized this as a critical connection-pooling anti-pattern. I manually refactored the code to instantiate a single, persistent RabbitMQ connection at server startup and reused the channel across all incoming requests.
