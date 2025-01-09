/* script for creating the curated property database 
    - first time create the database and schema using 
        admin (typically postgres) user
    - ensure user rezaware has priviledges to drop and
        create database and schema

    Contributors:
        nuwan.waidyanatha@rezgateway.com

*/
DROP DATABASE IF EXISTS property;
CREATE DATABASE property
 WITH OWNER = postgres
      ENCODING = 'UTF8'
      TABLESPACE = pg_default
      LC_COLLATE = 'en_US.UTF-8'
      LC_CTYPE = 'en_US.UTF-8'
      CONNECTION LIMIT = -1;

COMMENT ON DATABASE property IS 'hero property booking and room data for revenue management';

/* create admin user for tip with all priviledges */
CREATE USER rezaware WITH PASSWORD 'rezHERO';
GRANT ALL PRIVILEGES ON DATABASE "property" to rezaware;

ALTER DATABASE property OWNER TO rezaware;

/* Lakehouse schema applies to stage, historic, and curated databases */
DROP SCHEMA IF EXISTS curated CASCADE;
CREATE SCHEMA IF NOT EXISTS curated
        AUTHORIZATION rezaware;