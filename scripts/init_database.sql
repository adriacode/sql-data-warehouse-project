/*
=============================================================
Create Database and Schemas (PostgreSQL)
=============================================================
Script Purpose:
    This script creates a new database named "DataWarehouse".
    If the database already exists, it is dropped and recreated.
    After creation, three schemas are created:
        - bronze  (raw data)
        - silver  (cleaned / transformed data)
        - gold    (analytics-ready data)

WARNING:
    Running this script will drop the entire "DataWarehouse" database.
    All data in the database will be permanently deleted.
    Proceed with caution.
*/

/*
=============================================================
PART 1 — Create Database
IMPORTANT:
    This section must be executed while connected to another database
    (usually the default 'postgres' database).
=============================================================
*/

-- Terminate active connections to the target database
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'DataWarehouse'
  AND pid <> pg_backend_pid();

-- Drop database if it exists
DROP DATABASE IF EXISTS "DataWarehouse";

-- Create database
CREATE DATABASE "DataWarehouse";


/*
=============================================================
PART 2 — Connect to the Database
IMPORTANT:
    PostgreSQL does NOT support the USE command.

    You must connect to the database manually before continuing.

    pgAdmin:
        - Click on the database "DataWarehouse"
        - Open the Query Tool

    psql (terminal):
        \c "DataWarehouse"
=============================================================
*/


/*
=============================================================
PART 3 — Create Schemas
IMPORTANT:
    This section must be executed AFTER connecting to
    the "DataWarehouse" database.
=============================================================
*/

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
