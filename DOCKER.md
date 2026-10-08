# Docker Setup for ETL Scheduler

This guide explains how to run the ETL scheduler service in Docker.

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 1.29+

## Quick Start

### 1. Configure Environment Variables

Copy the example environment file and update it with your database credentials:

```bash
cp .env.example .env
```

Edit `.env` with your actual database connection details:

```env
DB_HOST=your-source-db-host
DB_PORT=5432
DB_NAME=your_operational_db
DB_USER=your_user
DB_PASSWORD=your_password

WH_DB_HOST=your-warehouse-db-host
WH_DB_PORT=5432
WH_DB_NAME=your_warehouse_db
WH_DB_USER=your_user
WH_DB_PASSWORD=your_password
```

### 2. Build and Start the Service

```bash
# Build the Docker image
docker-compose build

# Start the service in the background
docker-compose up -d

# View logs
docker-compose logs -f etl-scheduler
```

### 3. Verify It's Running

```bash
# Check container status
docker-compose ps

# View real-time logs
docker-compose logs -f etl-scheduler
```

## Usage

### Start the Service

```bash
docker-compose up -d
```

### Stop the Service

```bash
docker-compose stop
```

### Restart the Service

```bash
docker-compose restart etl-scheduler
```

### Remove Containers and Volumes

```bash
# Stop and remove containers (keeps data)
docker-compose down

# Remove everything including volumes
docker-compose down -v
```

### View Logs

```bash
# All logs
docker-compose logs

# Follow logs (real-time)
docker-compose logs -f etl-scheduler

# Last 100 lines
docker-compose logs --tail=100
```

## Configuration

### External Databases (Default)

By default, the compose file connects to external databases. Set the `*_DB_HOST` variables in `.env` to point to your existing databases.

### Using Docker Databases (Optional)

To run PostgreSQL databases as services, uncomment the `operational-db` and `warehouse-db` services in `docker-compose.yml`:

```yaml
services:
  operational-db:
    image: postgres:15-alpine
    # ... (uncommented from template)

  warehouse-db:
    image: postgres:15-alpine
    # ... (uncommented from template)
```

Then update `.env`:

```env
DB_HOST=operational-db
WH_DB_HOST=warehouse-db
```

## Scheduling

The ETL pipeline runs automatically at **4 PM EST (Monday-Friday)** inside the container.

- **Time Zone**: US/Eastern (Market Close)
- **Frequency**: Daily (trading days only)
- **Logs**: Stored in `./etl_logs/` (mounted volume)
- **Extracted Data**: Stored in `./extracted_data/` (mounted volume)

## Volume Mounts

The container uses the following volumes for data persistence:

| Local Path | Container Path | Purpose |
|---|---|---|
| `./etl_logs` | `/app/etl_logs` | Scheduler, extract, and load logs |
| `./extracted_data` | `/app/extracted_data` | Extracted CSV files |
| `./.env` | `/app/.env` | Environment configuration |

## Resource Limits

The container has resource limits configured:

- **CPU**: 1 core (limit), 0.5 cores (reservation)
- **Memory**: 1 GB (limit), 512 MB (reservation)

Adjust in `docker-compose.yml` under `services.etl-scheduler.deploy.resources` if needed.

## Troubleshooting

### Container Won't Start

```bash
# Check logs for errors
docker-compose logs etl-scheduler

# Check container status
docker-compose ps

# Verify environment variables are set
docker-compose config
```

### Database Connection Issues

Verify credentials in `.env`:

```bash
# Test connection from container
docker-compose exec etl-scheduler psql \
  -h $OPERATIONAL_DB_HOST \
  -U $OPERATIONAL_DB_USER \
  -d $OPERATIONAL_DB_NAME \
  -c "SELECT 1"
```

### Permission Issues

Ensure the local directories exist and are writable:

```bash
mkdir -p ./etl_logs ./extracted_data
chmod 755 ./etl_logs ./extracted_data
```

### Logs Not Appearing

Check mounted volume permissions:

```bash
ls -la ./etl_logs/
```

## Production Deployment

For production, consider:

1. **Secrets Management**: Use Docker secrets or a secrets management tool instead of `.env`
2. **Health Checks**: The container includes a health check endpoint
3. **Restart Policy**: Set to `unless-stopped` (default)
4. **Resource Limits**: Adjust CPU/memory based on your workload
5. **Logging**: Configure log rotation and aggregation
6. **Monitoring**: Add health check monitoring and alerting
7. **Database Backups**: Ensure backup strategies for warehouse data

Example for Kubernetes:

```yaml
containers:
- name: etl-scheduler
  image: your-registry/etl-scheduler:latest
  env:
    - name: OPERATIONAL_DB_HOST
      valueFrom:
        secretKeyRef:
          name: etl-secrets
          key: operational-db-host
    # ... other env vars
  resources:
    limits:
      cpu: "1"
      memory: "1Gi"
    requests:
      cpu: "500m"
      memory: "512Mi"
  livenessProbe:
    exec:
      command:
      - python
      - -c
      - import os; exit(0 if os.path.exists('/app/etl_logs/scheduler.log') else 1)
    initialDelaySeconds: 30
    periodSeconds: 60
```

## Support

For issues or questions, check:
- Container logs: `docker-compose logs -f`
- ETL logs: `./etl_logs/`
