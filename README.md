<!-- 🇧🇷 versão em português · [🇺🇸 english version](README.en.md) -->

# Olá, sou o Gustavo 👋

Engenheiro de dados com foco em **pipelines ETL/CDC end-to-end**, infraestrutura containerizada e sistemas multi-tenant.

Trabalho hoje no setor de TI de uma empresa do agronegócio, onde construí do zero a plataforma de dados que consolida operação de múltiplas filiais em tempo quase-real. Também entreguei sistemas web sob encomenda — entre eles um **ERP completo para revenda de motocicletas usadas**.

📍 **Londrina, PR · Brasil** · 🐧 Linux + Postgres + containers · 🐍 Python + 🐘 PostgreSQL

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

![Followers](https://img.shields.io/github/followers/fantiimm?label=Followers&style=for-the-badge&logo=github&color=181717)
![Repos](https://img.shields.io/badge/dynamic/json?label=Public%20Repos&query=%24.public_repos&url=https%3A%2F%2Fapi.github.com%2Fusers%2Ffantiimm&style=for-the-badge&logo=github&color=181717)
![Profile views](https://komarev.com/ghpvc/?username=fantiimm&label=Profile%20views&style=for-the-badge&color=181717)

---

## 🌊 Pipeline ETL/CDC multi-site (projeto profissional)

Plataforma de dados que replica em tempo quase-real **5 bases Postgres distribuídas geograficamente** para um data lake centralizado em cloud, alimentando dashboards operacionais e analíticos.

### Por que existe

Cinco unidades operacionais com bases Postgres locais — cada uma escrevendo independentemente. Sem visibilidade consolidada do negócio em tempo real, decisões eram tomadas com dados defasados em horas ou dias.

### Arquitetura Lambda — batch + streaming em paralelo

```
┌─ Filiais (Postgres 17, cada uma com schema próprio) ─┐
│                                                       │
│   filial-1   filial-2   filial-3   [+ outras]         │
│      │          │          │                          │
└──────┼──────────┼──────────┼──────────────────────────┘
       │ WAL/CDC     └──── batch SELECT (Airflow DAGs)
       ▼
┌─ Streaming layer (Cloud) ────────────────────────────┐
│   Debezium 3.x ─▶ Kafka 4.0 ─▶ Airflow daemons       │
│                                                       │
│       [particionamento por timestamp do fluxo]        │
│       [reconciler + auditor de coerência 24/7]        │
└───────────────────────────────────────────────────────┘
       ▼
┌─ Consumo (Postgres 17 + Superset 5.0) ───────────────┐
│   Tabelas fact_* particionadas                        │
│   Dashboards operacionais e analíticos                │
└───────────────────────────────────────────────────────┘
```

### Decisões arquiteturais que mais gosto

- **Lambda (batch + stream)** — stream resolve latência, batch reconcilia. Nenhum dos dois é fonte única; o auditor cruza ambos a cada 5 minutos
- **Debezium `snapshot.mode = no_data`** — captura WAL incremental sem ressincronização inicial completa (que travaria as origens)
- **Particionamento por timestamp do fluxo operacional**, não `created_at` — permite detectar registros "em andamento" pelo NULL do último timestamp
- **Containers Podman rootful** com compose dividido em 5 camadas (`infra`, `airflow`, `web`, `monitoring`, `daemons`) orquestradas por um único `stack.sh up`
- **Reconciler** que cobre o gap de atomicidade Kafka↔Postgres em commits de upsert idempotente
- **Provisão 100% reprodutível** — `provision.sh` + `bootstrap.sh` recriam o ambiente em VM nova partindo apenas do repo Git

### Stack técnico

| Camada | Tecnologia |
|---|---|
| Captura CDC | Debezium 3.0 (Kafka Connect) |
| Streaming | Apache Kafka 4.0 |
| Orquestração | Apache Airflow 2.9 (DAGs `replica_*` batch + DAGs `*_fato` stream) |
| Daemons ETL | Python 3.12 containerizados (1 por fluxo + 1 auditor) |
| Consumo | PostgreSQL 17 com particionamento nativo |
| BI | Apache Superset 5.0 |
| Monitoring | Prometheus + Grafana + postgres_exporter + kminion |
| Infra | Podman rootful em EC2 (Debian 13), Tailscale como malha overlay |

### Lições

- Stream sem reconciler não substitui batch — é complemento
- `ctid` não é único em tabela particionada (DELETE mata em todas as partições)
- Atomicidade Kafka(auto_commit)↔Postgres(batch) é um bug clássico; reconciler resolve sem reinventar
- Particionar pelo timestamp do fluxo dá visibilidade gratuita de "o que ainda está em andamento"

> *Código fechado por se tratar de operação corporativa, mas disposto a conversar sobre as decisões em entrevista técnica.*

---

## 🏍️ MotoGest — ERP web sob encomenda (projeto entregue)

Sistema de gestão **encomendado por um cliente** para operação de revenda de motocicletas usadas. Entreguei a aplicação completa, cobrindo o ciclo: **compra → estoque → manutenção → venda → financeiro → pós-venda**. Operação atual do cliente roda sobre essa base.

### Módulos entregues

| Módulo | O que faz |
|---|---|
| **Estoque** | Cadastro via wizard de 5 passos, fotos, documentos, checklist, custos acumulados, ficha PDF A4 com QR code, showroom público |
| **Compras** | Veículos, peças/serviços, consignações; parcelamento; 7 filtros em cascata |
| **Manutenção** | Pipeline cotação → previsto → em execução → finalizado, com itens, custos estimado vs real, fornecedor |
| **Vendas** | Wizard completo, parcelamento, contrato digital, ciclo "aguardando assinatura → ativa" |
| **Assinatura Digital** | Online (link por e-mail) e offline (upload PDF), em duas fases (contrato + procuração) |
| **Financeiro** | DRE, fluxo de caixa por conta, lançamentos, parcelas, inadimplência, retomadas |
| **CRM** | Funil de leads com pipeline kanban |
| **Contratos** | 4 templates tokenizados, geração e armazenamento de PDF |
| **Relatórios** | 4 relatórios analíticos com export PDF/CSV |
| **Auditoria** | Log automático via triggers PostgreSQL; histórico clicável em qualquer entidade |
| **Multi-tenant** | Isolamento em 3 camadas (JWT + ContextVar + Row-Level Security) |

### Decisão técnica que mais gosto — multi-tenant em três camadas

```
┌──────────────────────────────────────────────────┐
│  1. JWT — empresa_id no payload do access token  │
├──────────────────────────────────────────────────┤
│  2. ContextVar Python — propaga tenant async,    │
│     uma instância isolada por request            │
├──────────────────────────────────────────────────┤
│  3. PostgreSQL Row-Level Security — policies     │
│     na role app (non-superuser); query da DB     │
│     não enxerga registros de outra empresa,      │
│     mesmo que a app esqueça de filtrar           │
└──────────────────────────────────────────────────┘
```

Defesa em profundidade: se uma camada falhar, a próxima ainda protege.

### Stack técnico

| Camada | Tecnologia |
|---|---|
| Backend | Python 3.12 + FastAPI + SQLAlchemy 2.0 async |
| Banco | PostgreSQL 16 com Row-Level Security |
| Cache / Auth | Redis 7 (blacklist de tokens JWT) |
| Frontend | React 18 + TypeScript + Vite + shadcn/ui + Tailwind CSS |
| Geração de PDF | reportlab |
| Jobs agendados | APScheduler (alertas de vencimento, atraso de parcelas) |
| E-mail transacional | aiosmtplib via SMTP |
| Assinatura digital | signature_pad (canvas JS) + embed reportlab |
| Infra local | Podman + podman-compose (4 serviços) |
| Infra produção | AWS EC2 |

> *Repositório fechado — código pertence ao cliente que encomendou. Disposto a fazer demo do produto rodando ou conversar sobre arquitetura em entrevista técnica.*

---

## 📫 Contato

- 📧 **e-mail:** [gustavo.fantim.00@gmail.com](mailto:gustavo.fantim.00@gmail.com)
- 💼 **LinkedIn:** [Gustavo Fantim de Carvalho](https://www.linkedin.com/in/gustavofantimdecarvalho)
- 📍 **Londrina, PR — Brasil**

---

<sub>Alguns nomes de filiais, tecnologias específicas e métricas foram simplificados ou omitidos no projeto profissional para preservar confidencialidade. Os princípios técnicos e decisões arquiteturais descritos são reais.</sub>
