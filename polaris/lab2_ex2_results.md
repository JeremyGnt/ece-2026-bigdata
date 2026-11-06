# Polaris - Exercise 2 RBAC Results

## silver_reader
- Principal: silver_tester
- Principal role: silver_reader_role
- Catalog role: silver_reader
- Scope: namespace silver

## Access tests
- silver.orders read: allowed (2830 orders)
- gold.access_probe read: denied

## Conclusion
- Namespace-scoped Polaris permissions restrict catalog access.
- Polaris authorization does not replace S3 storage permissions.
