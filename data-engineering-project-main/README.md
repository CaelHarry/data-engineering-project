# SaaS Analytics Data Pipeline
An end-to-end batch analytics pipeline that ingests raw event data, transforms it using dbt, and orchestrates daily runs with Airflow. The project models product usage metrics such as DAU, signups, and feature engagement, making the data analytics-ready for dashboards and reporting.

#Project Overview
This project simulates a SaaS product analytics environment. It:

-Ingests raw event data (user activity events)
-Stores data in PostgreSQL
-Transforms raw data into analytics-ready fact tables using dbt
-Orchestrates transformations with Airflow
-Supports dashboard-ready KPI metrics (DAU, engagement, signups)

The goal was to build a production-style analytics stack similar to what modern data teams use.

#Architecture
Stack:

PostgreSQL – Data warehouse

dbt – Data transformations & modeling

Airflow – Orchestration

Docker – Containerized environment

Pipeline Flow:

Raw Events
→ PostgreSQL (raw layer)
→ dbt staging models
→ dbt fact tables (analytics layer)
→ Airflow scheduled daily execution
