# Apache Polaris - Lab 1: Deployment

## Objective
Deploy Apache Polaris 1.8.0 on Kubernetes using Helm.

## Components
- Apache Polaris REST catalog
- PostgreSQL database
- S3 object storage
- Kubernetes ConfigMaps and Secrets
- Bootstrap Job for RBAC configuration

## Deployment
The Helm chart is located in deployment/.

Run:
helm dependency build polaris/deployment
helm upgrade --install polaris polaris/deployment --wait

Required Kubernetes Secrets and ConfigMap must exist
before deploying the Helm chart.

## Access control
Two principals are configured:

- dev: data_engineer / catalog_contributor
- analyst: data_analyst / catalog_reader

## Validation
- Helm release deployed
- Polaris and PostgreSQL running
- Polaris health endpoint available
- Bootstrap Job completed

## Security
Credentials are stored in Kubernetes Secrets.
Generated client credentials are kept outside Git.
