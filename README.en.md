<!-- 🇺🇸 english version · [🇧🇷 versão em português](README.md) -->

# Hi, I'm Gustavo 👋

Data engineer focused on **end-to-end ETL/CDC pipelines**, containerized infrastructure, and multi-tenant systems.

Currently working in the IT department of an agribusiness company, where I built from scratch the data platform that consolidates operations from multiple branches in near-real-time. I also deliver custom web systems under contract — including a **full ERP for a used-motorcycle dealership**.

📍 **Londrina, PR · Brazil** · 🐧 Linux + Postgres + containers · 🐍 Python + 🐘 PostgreSQL

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Gustavo_Fantim_de_Carvalho-0A66C2?logo=linkedin&logoColor=white)](https://www.linkedin.com/in/gustavofantimdecarvalho)
[![Email](https://img.shields.io/badge/email-gustavo.fantim.00%40gmail.com-EA4335?logo=gmail&logoColor=white)](mailto:gustavo.fantim.00@gmail.com)

---

## 🛠️ Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-DD3E36?style=flat&logo=apache&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=flat&logo=redis&logoColor=white)
![Superset](https://img.shields.io/badge/Superset-20A6C9?style=flat&logo=apachesuperset&logoColor=white)
![Podman](https://img.shields.io/badge/Podman-892CA0?style=flat&logo=podman&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=flat&logo=linux&logoColor=black)
![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?style=flat&logo=typescript&logoColor=white)
![React](https://img.shields.io/badge/React-61DAFB?style=flat&logo=react&logoColor=black)

---

## 📊 GitHub

<p align="center">
  <img height="170" src="https://github-readme-stats.vercel.app/api?username=fantiimm&show_icons=true&include_all_commits=true&count_private=true&theme=tokyonight&hide_border=true" />
  <img height="170" src="https://github-readme-stats.vercel.app/api/top-langs/?username=fantiimm&layout=compact&theme=tokyonight&hide_border=true&langs_count=8" />
</p>

---

## 🌊 Multi-site ETL/CDC pipeline (professional project)

Data platform that replicates **5 geographically distributed Postgres databases** in near-real-time to a centralized data lake in the cloud, feeding operational and analytical dashboards.

### Why it exists

Five operational sites, each with its own local Postgres — all writing independently. Without consolidated business visibility in real time, decisions were being made on data hours or days old.

### Lambda architecture — batch + streaming in parallel

```
┌─ Branches (Postgres 17, each with its own schema) ───┐
│                                                       │
│   site-1    site-2    site-3   [+ others]            │
│     │         │         │                             │
└─────┼─────────┼─────────┼─────────────────────────────┘
      │ WAL/CDC    └─── batch SELECT (Airflow DAGs)
      ▼
┌─ Streaming layer (Cloud) ────────────────────────────┐
│   Debezium 3.x ─▶ Kafka 4.0 ─▶ Airflow daemons       │
│                                                       │
│       [partitioning by operational flow timestamp]    │
│       [24/7 reconciler + coherence auditor]           │
└───────────────────────────────────────────────────────┘
      ▼
┌─ Consumer (Postgres 17 + Superset 5.0) ──────────────┐
│   Partitioned fact_* tables                           │
│   Operational and analytical dashboards               │
└───────────────────────────────────────────────────────┘
```

### Architectural decisions I'm most proud of

- **Lambda (batch + stream)** — streaming solves latency, batch reconciles. Neither is the single source of truth; the auditor cross-checks both every 5 minutes
- **Debezium `snapshot.mode = no_data`** — captures incremental WAL without a full initial resync (which would freeze the sources)
- **Partitioning by the operational flow timestamp**, not `created_at` — enables free detection of "in-progress" records via NULL on the last timestamp
- **Rootful Podman containers** with compose split into 5 layers (`infra`, `airflow`, `web`, `monitoring`, `daemons`) orchestrated by a single `stack.sh up`
- **Reconciler** covering the Kafka↔Postgres atomicity gap in idempotent upsert commits
- **100% reproducible provisioning** — `provision.sh` + `bootstrap.sh` rebuild the entire environment in a fresh VM from the Git repo alone

### Tech stack

| Layer | Technology |
|---|---|
| CDC capture | Debezium 3.0 (Kafka Connect) |
| Streaming | Apache Kafka 4.0 |
| Orchestration | Apache Airflow 2.9 (`replica_*` batch DAGs + `*_fato` stream DAGs) |
| ETL daemons | Containerized Python 3.12 (one per flow + one auditor) |
| Consumer | PostgreSQL 17 with native partitioning |
| BI | Apache Superset 5.0 |
| Monitoring | Prometheus + Grafana + postgres_exporter + kminion |
| Infra | Rootful Podman on EC2 (Debian 13), Tailscale overlay mesh |

### Lessons learned

- Streaming without a reconciler doesn't replace batch — it complements it
- `ctid` isn't unique in a partitioned table (DELETE hits every partition)
- Kafka(auto_commit)↔Postgres(batch) atomicity is a classic bug; a reconciler solves it without reinventing the wheel
- Partitioning by flow timestamp gives you "what's still in-progress" visibility for free

> *Source code is closed since it's a corporate operation, but happy to discuss design decisions in a technical interview.*

---

## 🏍️ MotoGest — custom web ERP (delivered project)

Management system **commissioned by a client** to operate a used-motorcycle dealership. I delivered the complete application, covering the full cycle: **purchase → inventory → maintenance → sale → finance → after-sales**. The client's operation currently runs on this codebase.

### Delivered modules

| Module | What it does |
|---|---|
| **Inventory** | 5-step wizard registration, photos, documents, checklist, accumulated costs, A4 PDF spec sheet with QR code, public showroom |
| **Purchases** | Vehicles, parts/services, consignments; installment payments; 7-filter cascade |
| **Maintenance** | Pipeline quote → planned → in-progress → completed, with items, estimated vs actual costs, supplier |
| **Sales** | Full wizard, installments, digital contract, "awaiting signature → active" lifecycle |
| **Digital Signature** | Online (email link) and offline (PDF upload), in two phases (contract + power of attorney) |
| **Finance** | Income statement, per-account cash flow, journal entries, installments, delinquency, repossessions |
| **CRM** | Lead funnel with kanban pipeline |
| **Contracts** | 4 tokenized templates, PDF generation and storage |
| **Reports** | 4 analytical reports with PDF/CSV export |
| **Audit** | Automatic logging via PostgreSQL triggers; clickable history on any entity |
| **Multi-tenant** | 3-layer isolation (JWT + ContextVar + Row-Level Security) |

### Favorite technical decision — three-layer multi-tenant

```
┌──────────────────────────────────────────────────┐
│  1. JWT — company_id in access token payload     │
├──────────────────────────────────────────────────┤
│  2. Python ContextVar — propagates tenant async, │
│     one isolated instance per request            │
├──────────────────────────────────────────────────┤
│  3. PostgreSQL Row-Level Security — policies on  │
│     the app role (non-superuser); DB queries     │
│     can't see records from other tenants even    │
│     if the app forgets to filter                 │
└──────────────────────────────────────────────────┘
```

Defense in depth: if one layer fails, the next still protects.

### Tech stack

| Layer | Technology |
|---|---|
| Backend | Python 3.12 + FastAPI + SQLAlchemy 2.0 async |
| Database | PostgreSQL 16 with Row-Level Security |
| Cache / Auth | Redis 7 (JWT blacklist) |
| Frontend | React 18 + TypeScript + Vite + shadcn/ui + Tailwind CSS |
| PDF generation | reportlab |
| Scheduled jobs | APScheduler (due-date alerts, late-payment notifications) |
| Transactional email | aiosmtplib via SMTP |
| Digital signature | signature_pad (canvas JS) + embedded into reportlab |
| Local infra | Podman + podman-compose (4 services) |
| Production infra | AWS EC2 |

> *Repository is closed — code belongs to the commissioning client. Happy to demo the running product or discuss architecture in a technical interview.*

---

## 📫 Contact

- 📧 **email:** [gustavo.fantim.00@gmail.com](mailto:gustavo.fantim.00@gmail.com)
- 💼 **LinkedIn:** [Gustavo Fantim de Carvalho](https://www.linkedin.com/in/gustavofantimdecarvalho)
- 📍 **Londrina, PR — Brazil**

---

<sub>Some branch names, specific technologies and metrics were simplified or omitted in the professional project to preserve confidentiality. The technical principles and architectural decisions described are real.</sub>
