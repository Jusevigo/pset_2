
-- =====================================================
-- PSET 2 - INITIALIZATION OF SNOWFLAKE
-- Execute using ACCOUNTADMIN or an authorized admin role.
-- Idempotent: does not replace existing objects.
-- =====================================================

USE ROLE ACCOUNTADMIN;

-- 1. Project role
CREATE ROLE IF NOT EXISTS PSET2_ROLE;

-- 2. Virtual warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;

-- 3. Project database
CREATE DATABASE IF NOT EXISTS PSET2_DB;

-- 4. RAW schema
CREATE SCHEMA IF NOT EXISTS PSET2_DB.RAW;

-- 5. RAW source table
-- Structure obtained from GET_DDL in Snowflake.
CREATE TABLE IF NOT EXISTS PSET2_DB.RAW.AAPL_DAILY (
    SYMBOL VARCHAR(10),
    DATE DATE,
    CLOSE FLOAT,
    SOURCE VARCHAR(100),
    INGESTED_AT TIMESTAMP_NTZ(9)
);

-- 6. Database and warehouse permissions
GRANT USAGE ON WAREHOUSE COMPUTE_WH
    TO ROLE PSET2_ROLE;

GRANT USAGE ON DATABASE PSET2_DB
    TO ROLE PSET2_ROLE;

GRANT CREATE SCHEMA ON DATABASE PSET2_DB
    TO ROLE PSET2_ROLE;

-- 7. RAW schema permissions
GRANT USAGE, CREATE TABLE
    ON SCHEMA PSET2_DB.RAW
    TO ROLE PSET2_ROLE;

-- 8. RAW table permissions
GRANT SELECT, INSERT, UPDATE
    ON TABLE PSET2_DB.RAW.AAPL_DAILY
    TO ROLE PSET2_ROLE;

-- =====================================================
-- USER ASSIGNMENT
-- If PSET2_ROLE is not assigned to the login user,
-- execute the following separately, replacing the
-- placeholder with the actual Snowflake username:
--
-- GRANT ROLE PSET2_ROLE TO USER <SNOWFLAKE_USER>;
-- =====================================================
