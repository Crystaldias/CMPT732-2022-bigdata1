-- Open Street Map, using SUPER type
DROP TABLE IF EXISTS amenities;
CREATE TABLE amenities (
  amenid CHAR(11) PRIMARY KEY,
  lat FLOAT,
  lon FLOAT,
  ts TIMESTAMP,
  amenity VARCHAR NOT NULL,
  amname VARCHAR,
  tags SUPER
);

COPY amenities FROM 's3://S3_BUCKET/amenities/'
IAM_ROLE 'arn:aws:iam::AWS_ACCOUNT_ID:role/RedshiftS3Full'
FORMAT JSON 'auto';

-- Customers
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
  custid CHAR(11) PRIMARY KEY,
  custname VARCHAR,
  street VARCHAR,
  city VARCHAR,
  province CHAR(2),
  postalcode CHAR(6)
);

COPY customers FROM 's3://S3_BUCKET/customers/'
IAM_ROLE 'arn:aws:iam::AWS_ACCOUNT_ID:role/RedshiftS3Full'
FORMAT AS CSV;

-- Payment methods
DROP TABLE IF EXISTS paymentmethods;
CREATE TABLE paymentmethods (
  pmid CHAR(9) PRIMARY KEY,
  custid CHAR(11) REFERENCES customers(custid),
  number VARCHAR(20),
  expdate CHAR(5),
  mtype VARCHAR(6)
);

COPY paymentmethods FROM 's3://S3_BUCKET/paymentmethods/'
IAM_ROLE 'arn:aws:iam::AWS_ACCOUNT_ID:role/RedshiftS3Full'
FORMAT AS CSV;

-- Purchases within Redshift
DROP TABLE IF EXISTS purchases;
CREATE TABLE purchases (
  purchid CHAR(15) PRIMARY KEY,
  custid CHAR(11) REFERENCES customers(custid),
  pmid CHAR(9) REFERENCES paymentmethods(pmid),
  amenid CHAR(11) REFERENCES amenities(amenid),
  pdate DATE,
  amount DECIMAL(7,2)
);

COPY purchases FROM 's3://S3_BUCKET/purchases/'
IAM_ROLE 'arn:aws:iam::AWS_ACCOUNT_ID:role/RedshiftS3Full'
FORMAT AS CSV;

-- Greater Vancouver postal code 3-letter prefixes
DROP TABLE IF EXISTS greater_vancouver_prefixes;
CREATE TABLE greater_vancouver_prefixes (
  city VARCHAR(15),
  pcprefix CHAR(3)
);

COPY greater_vancouver_prefixes FROM 's3://S3_BUCKET/redshift-tables/greater_vancouver_prefixes.csv'
IAM_ROLE 'arn:aws:iam::AWS_ACCOUNT_ID:role/RedshiftS3Full'
FORMAT AS CSV;

/*--------------------- External tables -----------------------------

The following CREATE statements must be performed one at a time.

*/

-- External schema
CREATE EXTERNAL SCHEMA s3ext
FROM DATA CATALOG 
DATABASE 's3ext'
IAM_ROLE 'arn:aws:iam::AWS_ACCOUNT_ID:role/RedshiftS3Full'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Purchases as external table partitioned by date
CREATE EXTERNAL TABLE s3ext.purchases (
  purchid CHAR(15),
  custid CHAR(11),
  pmid CHAR(9),
  amenid CHAR(11),
  amount DECIMAL(7,2)
)
PARTITIONED BY (pdate DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED by ','
STORED AS TEXTFILE
LOCATION 's3://S3_BUCKET/ext-purchases/';

/*
  Create EXTERNAL TABLE for amenities

  This is not used but is included here in case someone wants to
  experiment with it.  External tables cannot use the SUPER
  datatype but require STRUCT instead. See
  https://docs.aws.amazon.com/redshift/latest/dg/tutorial-query-nested-data.html
  for syntax details.

  For a tag field name to be recognized in SQL queries, it has to be listed
  in the STRUCT declaration. The list used here was derived by running
  Amazon Glue on the amenities file but it could also have been generated
  by writing a simple Python program.

  In a SUPER datatype, if you reference a field that is not present, it returns
  NULL. In a STRUCT datatype however, if you reference a field that is not
  defined in the STRUCT schma, you get an SQL syntax error.
*/
CREATE EXTERNAL TABLE s3ext.amenities (
  amenid CHAR(11),
  lat FLOAT,
  lon FLOAT,
  ts TIMESTAMP,
  amenity VARCHAR,
  amname VARCHAR,
  tags STRUCT<brand_wikidata: varchar, official_name: varchar, addr_housenumber: varchar, brand_wikipedia: varchar, opening_hours: varchar,
    cuisine: varchar, addr_street: varchar, takeaway: varchar, brand: varchar, denomination: varchar, religion: varchar, operator: varchar,
    website: varchar, fuel_octane_91: varchar, fuel_octane_89: varchar, fuel_diesel: varchar, fuel_octane_87: varchar, male: varchar,
    phone: varchar, addr_postcode: varchar, addr_city: varchar, parking: varchar, access: varchar, toilets_wheelchair: varchar, wheelchair: varchar,
    name_en: varchar, created_by: varchar, level: varchar, indoor: varchar, healthcare: varchar, description: varchar, healthcare_speciality: varchar,
    backrest: varchar, source: varchar, fuel_octane_94: varchar, covered: varchar, self_service: varchar, addr_unit: varchar, name_zh: varchar,
    facebook: varchar, emergency: varchar, fax: varchar, internet_access: varchar, drive_through: varchar, surveillance: varchar, atm: varchar,
    fee: varchar, is_in_city: varchar, wikidata: varchar, alt_name: varchar, public_transport: varchar, ferry: varchar, shop: varchar,
    shelter_type: varchar, outdoor_seating: varchar, email: varchar, capacity: varchar, ref: varchar, network: varchar, collection_times: varchar,
    unisex: varchar, toilets_disposal: varchar, building: varchar, leisure: varchar, material: varchar, recycling_glass_bottles: varchar,
    recycling_type: varchar, recycling_cans: varchar, inscription: varchar, historic: varchar, memorial: varchar, entrance: varchar,
    vending: varchar, female: varchar, note: varchar, sport: varchar, fuel_gasoline: varchar, fuel_propane: varchar, bicycle_parking: varchar,
    alt: varchar, colour: varchar, seats: varchar, post_box_type: varchar, smoking: varchar, brand_en: varchar, brand_zh: varchar, image: varchar,
    bus: varchar, shelter: varchar, notes: varchar, tactile_paving: varchar, highway: varchar, tourism: varchar, url: varchar,
    ferry_terminal: varchar, type: varchar, drinking_water: varchar, addr_province: varchar, old_name: varchar, dispensing: varchar,
    wikipedia_en: varchar, layer: varchar, service_bicycle_pump: varchar, internet_access_fee: varchar, quantity: varchar, payment_coins: varchar,
    name_fr: varchar, category: varchar, currency_USD: varchar, currency_CAD: varchar, currency_MXN: varchar, currency_DKK: varchar,
    currency_CHF: varchar, currency_EUR: varchar, currency_RUB: varchar, currency_others: varchar, currency_HUF: varchar, currency_GBP: varchar,
    currency_JPY: varchar, currency_PLN: varchar, currency_AUD: varchar, currency_THB: varchar, short_name: varchar, payment_credit_cards: varchar,
    payment_debit_cards: varchar, artist_name: varchar, lit: varchar, delivery: varchar, owner: varchar, recycling_clothes: varchar,
    recycling_belts: varchar, recycling_gas_bottles: varchar, man_made: varchar, microbrewery: varchar, air_conditioning: varchar, bar: varchar,
    brewery: varchar, noname: varchar, recycling_glass: varchar>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true',
  'paths'='amenid,lat,lon,timestamp,amenity,amname,tags')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://S3_BUCKET/amenities/';
