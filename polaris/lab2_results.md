# Polaris - Lab 2 Results

## Catalog connections
- Hadoop catalog namespaces: ['silver']
- Polaris catalog namespaces: []
- lab uses the Hadoop catalog on S3.
- polaris uses the Polaris REST API with dev credentials.

## Register an existing Iceberg table
- Metadata version: v4
- Registration: New registration
- Hadoop orders: 2830
- Polaris orders: 2830
- Storage objects before: 17
- Storage objects after: 17
- Registration did not copy data files.

## Create table through Polaris
- Source users: 50
- Polaris users: 50
- Location: s3://user-j-gonnet-ece/warehouse/silver/polaris_users

## Schema change as dev
- Added country STRING to polaris.silver.users.
- Snapshots before: 1
- Snapshots after: 1
- Schema changes update table metadata.
