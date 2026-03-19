# Greg Lewis | Modern Analytics Leadership

<p align="left">
  <a href="https://www.postgresql.org/docs/" target="_blank"><img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-Analytics%20Engineering-336791?logo=postgresql&logoColor=white"></a>
  <a href="https://docs.getdbt.com/docs/introduction" target="_blank"><img alt="ELT Design" src="https://img.shields.io/badge/ELT-Modular%20SQL-FF694B?logo=dbt&logoColor=white"></a>
  <a href="https://www.atlassian.com/software/bitbucket/pipelines" target="_blank"><img alt="Bitbucket Pipelines" src="https://img.shields.io/badge/CI%2FCD-Bitbucket%20Pipelines-0052CC?logo=bitbucket&logoColor=white"></a>
  <a href="https://www.tableau.com/" target="_blank"><img alt="Tableau Ready" src="https://img.shields.io/badge/BI-Tableau%20Ready-E97627?logo=tableau&logoColor=white"></a>
</p>

## From data to metrics that impact the bottom line.

This repository showcases how to approach analytics as a business leader, translating messy operational workflows into decision-grade data products, KPI frameworks, and deployment-ready SQL pipelines.

For businesses needing to step into the modern age of data, these examples reflect a rock-solid way to minimize noise and maximize change:

Strategic mindset:
- **Executive-minded analytics strategy** tied to business outcomes.
- **Hands-on analytics engineering** across staging, intermediate, fact, and metric layers.
- **Operational performance management** for revenue, service, and process improvement teams.
- **Data quality and deployment discipline** that supports scalable, trustworthy reporting.
- **Cross-functional thinking** across finance, operations, sales, and healthcare revenue cycle domains.

Mission critical deliverables:
- KPI architectures for executive dashboards.
- Funnel and pipeline analytics for go-to-market teams.
- Revenue cycle / denial / claims analytics for healthcare operators.
- SLA, workflow, and throughput measurement for operational teams.
- Production-ready SQL assets and repeatable deployment patterns.

---

## What is in here:

### 1) Revenue Cycle Management analytics
The `RCM` project models the healthcare claims lifecycle, denial categorization, and root-cause analysis needed to improve collections, reduce write-offs, and prioritize interventions. It includes:

- staging and intermediate transformations for claims, payments, and denials.
- fact tables for claims lifecycle and denial analytics.
- a prioritization query for identifying the highest-impact denial interventions.
- automated data-quality checks and deployment scripts for production execution.

**Representative assets:**
- [`RCM/fct_claims_lifecycle.sql`](./RCM/fct_claims_lifecycle.sql)
- [`RCM/fct_denial_analytics.sql`](./RCM/fct_denial_analytics.sql)
- [`RCM/denial_root_cause_priority.sql`](./RCM/denial_root_cause_priority.sql)
- [`RCM/data_quality_tests.sql`](./RCM/data_quality_tests.sql)

### 2) Sales Operations and pipeline performance analytics
The `Sales Ops` project demonstrates a clean analytics stack for pipeline visibility, conversion performance, rep productivity, and RFP execution. It includes:

- staging models for leads, opportunities, accounts, activities, quota, and RFPs.
- intermediate models for enriched pipeline, funnel conversion, rep productivity, and RFP lifecycle analysis.
- fact and aggregate metric tables at daily, weekly, monthly, and quarterly grains.
- CI/CD and deployment automation for a repeatable analytics workflow.

**Representative assets:**
- [`Sales Ops/sql/03_marts/fct_sales_pipeline_lifecycle.sql`](./Sales%20Ops/sql/03_marts/fct_sales_pipeline_lifecycle.sql)
- [`Sales Ops/sql/03_marts/fct_rfp_operations.sql`](./Sales%20Ops/sql/03_marts/fct_rfp_operations.sql)
- [`Sales Ops/sql/03_marts/metrics_salesops_daily.sql`](./Sales%20Ops/sql/03_marts/metrics_salesops_daily.sql)
- [`Sales Ops/scripts/deploy_salesops.sh`](./Sales%20Ops/scripts/deploy_salesops.sh)

---

## Leadership lens: what potential employers should see here

This repository is intentionally structured to show more than technical competence. It reflects how I think about analytics leadership at scale:

### Business alignment first
Each data product is tied to a real operational question such as:
- Where is revenue leaking?
- Which workflows are slowing speed-to-close?
- Which teams are hitting SLA and throughput expectations?
- Which interventions will generate the biggest financial return?

### Metrics that drive action
These models go beyond descriptive reporting. They are designed to surface:
- performance flags,
- operational bottlenecks,
- conversion rates,
- recovery opportunities,
- capacity and productivity signals,
- and decision-ready aggregates for executive review.

### Repeatable operating model
A strong analytics function needs more than dashboards. This repo includes patterns for:
- standardized SQL layering,
- indexing and performance considerations,
- deployment scripting,
- test execution,
- and pipeline orchestration through Bitbucket Pipelines.

### Cross-functional credibility
The examples span domains that require partnering with stakeholders in:
- sales leadership,
- operations,
- finance,
- revenue cycle teams,
- and executive leadership.

That is the level where I add the most value: connecting strategy, process, analytics, and adoption.

---

## Core capabilities represented in this repo

| Capability | Evidence in repository |
|---|---|
| Analytics strategy | KPI marts and business-rule flags designed for operational decision-making |
| Analytics engineering | Modular SQL architecture across staging, intermediate, and mart layers |
| Revenue cycle analytics | Claims lifecycle, denial categorization, recovery analysis, and root-cause prioritization |
| Sales / GTM analytics | Funnel conversion, pipeline performance, rep productivity, and RFP operations |
| Data quality management | SQL-based data quality tests and monitoring inserts |
| Deployment discipline | Shell-based deployment scripts and Bitbucket pipeline definitions |
| Executive reporting readiness | Aggregations at daily, weekly, monthly, and quarterly grains |

---

## Repository map

```text
GregLewis/
├── RCM/
│   ├── staging + intermediate + mart SQL for claims / denials / payments
│   ├── data quality tests
│   ├── production deployment script
│   └── analysis query for denial intervention prioritization
├── Sales Ops/
│   ├── staging + intermediate + mart SQL for pipeline / RFP / productivity analytics
│   ├── tests for data quality and schema validation
│   ├── deployment automation
│   └── Bitbucket pipeline configuration
└── README.md
```

---

## How this helps businesses

If you are hiring for a senior analytics leader or engaging a consultant, this portfolio reflects the type of outcomes I help create:

### Can be built:
Create or scale an analytics function that:
- aligns roadmaps to business priorities,
- improves trust in KPI reporting,
- creates stronger visibility into operational performance,
- upgrades reporting from reactive to decision-driving,
- and establishes better partnership between analytics and the business.

### Can be delivered:
Implement:
- KPI framework design,
- data model redesign,
- SQL pipeline modernization,
- Tableau / BI-ready semantic layers,
- workflow and SLA analytics,
- revenue integrity and denial performance analytics,
- and operating reviews that connect data to action plans.

---

## Highlighted technologies and practices

- **SQL / PostgreSQL** for transformation and analytics modeling.
- **Layered ELT design** using staging, intermediate, and mart patterns.
- **Operational KPI modeling** for executive and manager-level scorecards.
- **CI/CD** with Bitbucket Pipelines.
- **Deployment scripting** with Bash.
- **BI-consumption readiness** for tools such as Power BI, Tableau, Looker, ThoughtSpot, QuickSights, Alteryx, and Informatica.

---

## A few strong examples to review first

If you only review a handful of files, start here:

1. **Healthcare claims lifecycle fact model**  
   [`RCM/fct_claims_lifecycle.sql`](./RCM/fct_claims_lifecycle.sql)
2. **Denial analytics fact model**  
   [`RCM/fct_denial_analytics.sql`](./RCM/fct_denial_analytics.sql)
3. **Denial intervention prioritization analysis**  
   [`RCM/denial_root_cause_priority.sql`](./RCM/denial_root_cause_priority.sql)
4. **Sales pipeline lifecycle fact model**  
   [`Sales Ops/sql/03_marts/fct_sales_pipeline_lifecycle.sql`](./Sales%20Ops/sql/03_marts/fct_sales_pipeline_lifecycle.sql)
5. **Daily Sales Ops KPI model**  
   [`Sales Ops/sql/03_marts/metrics_salesops_daily.sql`](./Sales%20Ops/sql/03_marts/metrics_salesops_daily.sql)
6. **Sales Ops deployment automation**  
   [`Sales Ops/scripts/deploy_salesops.sh`](./Sales%20Ops/scripts/deploy_salesops.sh)

---

## Final note

This is a portfolio repository, but it is also a statement about how to deliver powerful, high-impact simplicity from disparate and complex data sources:

> Build analytics that hold up under scrutiny, drive operations, and scale with growth.

These are metrics that operate comfortably in both the **boardroom conversation** and the **SQL details underneath it**.
