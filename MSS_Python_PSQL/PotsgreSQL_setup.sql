-- Create database AdventureWorks to store the incoming data
CREATE DATABASE 
    "AdventureWorks"
WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Spanish_Mexico.1252'
    LC_CTYPE = 'Spanish_Mexico.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

-- Create ETL user
CREATE USER etl WITH PASSWORD 'demopass';
-- Grant connect
GRANT CONNECT ON DATABASE 'AdventureWorks' TO etl;
-- grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl;
