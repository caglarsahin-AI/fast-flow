# PayCore.DWH Airflow Project

This repository contains Python-based ETL processes orchestrated by Apache Airflow, running in Docker on Linux servers.

---

## Table of Contents

1. [Overview](#overview)
2. [Environment](#environment)
3. [Installation](#installation)
4. [Directory Structure](#directory-structure)
5. [Docker & Docker Compose](#docker--docker-compose)
6. [Starting Airflow](#starting-airflow)
7. [CI/CD (Jenkins)](#cicd-jenkins)
8. [ETL Projects & DAGs](#etl-projects--dags)
9. [Common Git Commands](#common-git-commands)

---

## Overview

- **Repo Name:** `airflow_project`
- **Purpose:** Schedule and execute Python ETL workflows as Airflow DAGs.
- **Host:** Linux servers (production and test).

---

## Environment

- **Production Server:** `10.62.150.102` (Linux)
- **Test Server:** `10.27.158.102` (Linux)

All services are deployed via Docker Compose on these hosts.

---


## Installation

```bash
# Clone the repository
git clone https://git.paycore.com/scm/dwh/airflow_project.git .
cd airflow_project

# (Optional) Install Python dependencies locally
pip install -r requirements.txt
```

---

## Directory Structure

```
Airflow_project/
├── dags/            # Airflow DAG definitions
├── logs/            # Runtime logs
├── plugins/         # Custom Airflow plugins (e.g. send_email_custom.py)
├── projects/        # Python packages/modules for ETL logic
├── airflow.cfg      # Airflow configuration
├── docker-compose.yml
├── Dockerfile       # Docker build for custom Airflow image
├── Jenkinsfile      # CI/CD pipeline definition
├── requirements.txt # Python dependencies
└── webserver_config.py
```

---

## Docker & Docker Compose

**Dockerfile**

```dockerfile
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY requirements.txt /tmp/requirements.txt
COPY plugins/send_email_custom.py /opt/airflow/plugins/send_email_custom.py
RUN pip install -r /tmp/requirements.txt
```

**docker-compose.yml**

```yaml
version: '3.8'
services:
  postgres:
    image: artifacts.paycore.com/postgres:13
    container_name: airflow_postgres
    restart: always
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"
    volumes:
      - ./pgdata:/var/lib/postgresql/data

  airflow-init:
    image: artifacts.paycore.com/apache/airflow:2.7.2-python3.9.11
    container_name: airflow_init
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    entrypoint: /bin/bash -c "airflow db init"
    volumes:
      - .:/opt/airflow
      - ./dags:/opt/airflow/dags

  airflow-webserver:
    image: artifacts.paycore.com/apache/airflow:2.7.2-python3.9.11
    container_name: airflow_webserver
    restart: always
    depends_on:
      - postgres
      - airflow-init
    environment:
      TZ: Europe/Istanbul
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__DEFAULT_TIMEZONE: Europe/Istanbul
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__WEBSERVER__DEFAULT_USER_USERNAME: admin
      AIRFLOW__WEBSERVER__DEFAULT_USER_PASSWORD: admin
    ports:
      - "8080:8080"
    volumes:
      - .:/opt/airflow
      - ./dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock
    command: webserver

  airflow-scheduler:
    image: artifacts.paycore.com/apache/airflow:2.7.2-python3.9.11
    container_name: airflow_scheduler
    restart: always
    depends_on:
      - postgres
      - airflow-init
    environment:
      TZ: Europe/Istanbul
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__DEFAULT_TIMEZONE: Europe/Istanbul
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    volumes:
      - .:/opt/airflow
      - ./dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock
    command: scheduler
```

```bash
# Start all services in detached mode
 docker-compose up -d --build

# Check service status
docker-compose ps

# Tail logs for webserver
docker-compose logs -f airflow-webserver
```

---

## Starting Airflow

- **Web UI:** `http://<server_ip>:8080`\
  Default credentials: `admin` / `admin`
- The scheduler continuously scans the `dags/` folder and triggers tasks as defined.

---

## CI/CD (Jenkins)

**Jenkinsfile**

```groovy
pipeline {
  agent { label "Slave5" }
  environment {
    BASE_IMAGE = "apache/airflow:2.7.2-python3.9"
    BASE_IMAGE_VERSION = "2.7.2-python3.9"
    REGISTRY_URL = "artifacts.paycore.com"
    IMAGE_NAME = "apache/airflow"
    DOCKER_CREDENTIALS_ID = "docker-publisher"
    ENVIRONMENT = "prod"
  }
  stages {
    stage('Build') {
      steps {
        sh "docker build --build-arg BASE_IMAGE=${BASE_IMAGE} -t ${REGISTRY_URL}/${IMAGE_NAME}:${BASE_IMAGE_VERSION}.${BUILD_NUMBER}-${ENVIRONMENT} ."
      }
    }
    stage('Push') {
      steps {
        docker.withRegistry("https://${REGISTRY_URL}", "${DOCKER_CREDENTIALS_ID}") {
          sh "docker push ${REGISTRY_URL}/${IMAGE_NAME}:${BASE_IMAGE_VERSION}.${BUILD_NUMBER}-${ENVIRONMENT}"
        }
      }
    }
  }
}
```

---

## ETL Projects & DAGs

- Place each DAG definition (`.py`) into the `dags/` folder.
- Organize your Python modules/packages under `projects/` and import them in DAGs.
- Custom email functionality is implemented in `plugins/send_email_custom.py`.

---

## Common Git Commands

```bash
# Clone the repo
git clone https://git.paycore.com/scm/dwh/airflow_project.git .

# Switch to master branch
git checkout master

# Create and switch to a new branch
git checkout -b test

# Stage and commit changes
git add .
git commit -m "Descriptive commit message"

# Push branch to remote
git push -u origin <branch_name>

# List remotes
git remote -v

# Add a new remote
git remote add origin https://git.paycore.com/scm/dwh/airflow_project.git

# Update remote URL
git remote set-url origin <new_url>

# List branches
git branch

# Switch branches
git checkout <branch_name>
```

---

*Run all commands from the repository root directory.*

