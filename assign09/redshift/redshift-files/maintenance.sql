-- Drop all Redshift tables
DROP TABLE IF EXISTS amenities CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS paymentmethods CASCADE;
DROP TABLE IF EXISTS greater_vancouver_prefixes CASCADE;
DROP TABLE IF EXISTS purchases CASCADE;

-- Drop the external schema and database, including all tables
-- This only removes Redshift's access to S3.  The underlying
-- S3 objects are unaffected.
DROP SCHEMA IF EXISTS s3ext DROP EXTERNAL DATABASE CASCADE;
