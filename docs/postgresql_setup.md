## Linux (Ubuntu 24.04+)

```bash
su root

apt-get update

apt-get install -y postgresql

systemctl enable postgresql

# Generate a random password for the sidecar user if you need one:
head /dev/urandom | LC_ALL=C tr -dc A-Za-z0-9 | head -c64

su postgres

# enter psql cli and run the create user/database script below
psql

# once the setup script has run, test that it works
psql --host localhost --dbname sidecar --user sidecar --password
```

### Create the database and user
```sql
CREATE USER sidecar WITH PASSWORD 'your_secure_password_here';
CREATE DATABASE sidecar;
GRANT ALL PRIVILEGES ON DATABASE sidecar TO sidecar;
ALTER DATABASE sidecar OWNER TO sidecar;

-- Connect to sidecar database and set ownership of public schema
\c sidecar
ALTER SCHEMA public OWNER TO sidecar;

-- Grant all privileges on existing tables and sequences (there shouldnt be any if this is brand new) 
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sidecar;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sidecar;

-- Grant permissions for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT ALL ON TABLES TO sidecar;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT ALL ON SEQUENCES TO sidecar;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT ALL ON FUNCTIONS TO sidecar;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
GRANT ALL ON TYPES TO sidecar;
```

### Tuned PostgreSQL parameters

```text
shared_buffers = 4GB # aim for 25% of RAM
work_mem = 2GB # per connection
effective_cache_size = 12GB # aim for 75% of RAM
maintenance_work_mem = 1GB # 5% of RAM, max 2GB
max_wal_size = 16GB
min_wal_size = 4GB
random_page_cost = 1.1 # for SSDs, otherwise leave it as the default value
```