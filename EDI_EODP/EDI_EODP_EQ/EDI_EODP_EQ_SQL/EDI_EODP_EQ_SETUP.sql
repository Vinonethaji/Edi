/*
Pre-requisite 1: Define variables for specifying file type, field delimiter, date format, encoding,
file format, S3 bucket url, snowflake warehouse, and scheduled time.
*/

SET file_type = 'CSV';
SET field_delimiter = '\t';
SET timestamp_format = 'YYYY/MM/DD HH24:MI:SS';
SET date_format = 'YYYY/MM/DD';
SET encoding = 'iso-8859-1';
SET file_format = 'EDI_EODP.OPERATIONS.EODP_EQ_FILE_FORMAT';
SET bucket_url = 's3://edi-snowflake-mkt/prices_eq/';

/*  
Pre-requisite 2: Database and Schema creation
Create the necessary database and schema.

Create the database EDI_EODP
*/
CREATE DATABASE IF NOT EXISTS EDI_EODP;
/*
Create the schema: OPERATIONS Schema with Landing table, Temp Table, Error Table, Stages, Pipes, Streams, Stored Procedures, and Tasks.
*/

CREATE SCHEMA IF NOT EXISTS EDI_EODP.OPERATIONS;

/*
The PROD schema contains Final tables and Secure Views.
*/

CREATE SCHEMA IF NOT EXISTS EDI_EODP.PROD;

/*  
Pre-requisite 3: Table Creation
Create Landing, Temporary, Error, and Final tables for EODP_EQ data.
Select the EDI_EODP_WH warehouse. 
*/

USE WAREHOUSE EDI_EODP_WH;

/*
Creation of Landing table for EODP_EQ data
*/

CREATE TABLE EDI_EODP.OPERATIONS.EODP_EQ_INC_LANDING (
MIC VARCHAR (13) NOT NULL,
pricefilesymbol CHAR (60),
Isin CHAR (12),
Currency CHAR (3),
PriceDate VARCHAR (10),
Open DOUBLE,
High DOUBLE,
Low DOUBLE,
Close DOUBLE,
Mid DOUBLE,
Ask DOUBLE,
Last DOUBLE,
Bid DOUBLE,
Bidsize DOUBLE,
Asksize DOUBLE,
TradedVolume DOUBLE,
SecID INT,
MktCloseDate VARCHAR (10),
VolFlag CHAR (1),
Issuername VARCHAR(70),
SecTyCD CHAR(3),
SecurityDesc VARCHAR(70),
Sedol CHAR(7),
Uscode CHAR (9),
PrimaryExchgCD CHAR(6),
ExchgCD CHAR(6),
TradedValue DOUBLE,
TotalTrades INTEGER,
Comment VARCHAR(50)
);

/*
Creation of Temporary table for EODP_EQ data
The temporary table is utilized to empty the stream and maintain data consistency from the stream to the final table.
This table will be regularly truncated after feeding the data into the final table.
*/
CREATE TABLE EDI_EODP.OPERATIONS.EODP_EQ_INC_TEMP (
MIC VARCHAR (13),
pricefilesymbol CHAR (60),
Isin CHAR (12),
Currency CHAR (3),
PriceDate VARCHAR (10),
Open DOUBLE,
High DOUBLE,
Low DOUBLE,
Close DOUBLE,
Mid DOUBLE,
Ask DOUBLE,
Last DOUBLE,
Bid DOUBLE,
Bidsize DOUBLE,
Asksize DOUBLE,
TradedVolume DOUBLE,
SecID INT,
MktCloseDate VARCHAR (10),
VolFlag CHAR (1),
Issuername VARCHAR(70),
SecTyCD CHAR(3),
SecurityDesc VARCHAR(70),
Sedol CHAR(7),
Uscode CHAR (9),
PrimaryExchgCD CHAR(6),
ExchgCD CHAR(6),
TradedValue DOUBLE,
TotalTrades INTEGER,
Comment VARCHAR(50),
METADATA$ACTION VARCHAR,
METADATA$ISUPDATE VARCHAR,
METADATA$ROW_ID VARCHAR  
);

/*
Creation of Error table for EODP_EQ data
The Error table contains rows of files that failed during the process.
*/
CREATE TABLE EDI_EODP.OPERATIONS.EODP_EQ_INC_ERROR (
MIC VARCHAR (13),
pricefilesymbol CHAR (60),
Isin CHAR (12),
Currency CHAR (3),
PriceDate VARCHAR (10),
Open DOUBLE,
High DOUBLE,
Low DOUBLE,
Close DOUBLE,
Mid DOUBLE,
Ask DOUBLE,
Last DOUBLE,
Bid DOUBLE,
Bidsize DOUBLE,
Asksize DOUBLE,
TradedVolume DOUBLE,
SecID INT,
MktCloseDate VARCHAR (10),
VolFlag CHAR (1),
Issuername VARCHAR(70),
SecTyCD CHAR(3),
SecurityDesc VARCHAR(70),
Sedol CHAR(7),
Uscode CHAR (9),
PrimaryExchgCD CHAR(6),
ExchgCD CHAR(6),
TradedValue DOUBLE,
TotalTrades INTEGER,
Comment VARCHAR(50),
METADATA$ACTION VARCHAR,
METADATA$ISUPDATE VARCHAR,
METADATA$ROW_ID VARCHAR, 
CREATED_BY VARCHAR(255),
CREATED_AT TIMESTAMP_NTZ(9)
);

/*
Creation of Final table for EODP_EQ data with Column Descriptions
The Final table for EODP_EQ data is created, including descriptions for each column.

*/
CREATE TABLE EDI_EODP.PROD.EODP_EQ_INC(
MIC CHAR (6) NOT NULL COMMENT 'Swift`s ISO Standard 10383 Market Identification Code',
pricefilesymbol CHAR (60) COMMENT 'Ticker/localcode',
Isin CHAR (12) COMMENT 'ISIN ISO Standard 6166',
Currency CHAR (3) NOT NULL COMMENT 'ISO Standard 4217 Code for Base Currency',
PriceDate DATE NOT NULL COMMENT 'Real date of price - note that this is does not necessarily match the closedate implied in the file name',
Open DOUBLE COMMENT 'The first price of the trading day or session (can bederived,not necessarily the first trade/transaction)',
High DOUBLE COMMENT 'Day`s high price',
Low DOUBLE COMMENT 'Day`s low price',
Close DOUBLE NOT NULL COMMENT 'The official close of the trading day or session (can be derived,not necessarily the last trade/transaction)',
Mid DOUBLE COMMENT 'The average of the bid and ask prices rounded up to the price format allowed for that tradableinstrument/currency (London SE only)',
Ask DOUBLE COMMENT 'The selling price (ask/offer) for securities in the market',
Last DOUBLE COMMENT 'The most recent (last) trade/transaction price of the trading day or session',
Bid DOUBLE COMMENT 'the buying price for securities in the market',
Bidsize DOUBLE COMMENT 'Quantity of a security that investors are willing to purchase at a specified bid price',
Asksize DOUBLE COMMENT 'Quantity of a security that people are willing to sell at a specified ask price',
TradedVolume DOUBLE COMMENT 'The number of shares or contracts traded during thetrading day or session',
SecID INT COMMENT 'Security ID from EDI`s Worldwide Corporate Action Database to link into Corporate Actions',
MktCloseDate DATE NOT NULL COMMENT 'Market Close Date',
VolFlag CHAR (1) COMMENT 'Relates to trading Volume, A= Absolute, T= Stated in thousands trading day or session',
Issuername VARCHAR(70) COMMENT 'EDI`s Masterfile Issuer name if link established',
SecTyCD CHAR(3) COMMENT 'EDI`s Masterfile Security Type Code if link established. EQS (EQ), PRF (Preference), DR (Depositary Receipt)',
SecurityDesc VARCHAR(70) COMMENT 'EDI`s Masterfile Security Description if link established',
Sedol CHAR(7) COMMENT 'Sedol Code',
Uscode CHAR (9) COMMENT 'Extracted from US an Canadian Isins',
PrimaryExchgCD CHAR(6) COMMENT 'EDI proprietary Exchange Code of primary Listing if known',
ExchgCD CHAR(6) NOT NULL COMMENT 'EDI proprietary Exchange Code',
TradedValue DOUBLE COMMENT 'The total value of shares or contracts traded during the trading day or session',
TotalTrades INTEGER COMMENT 'Total numbers of trades in this stock',
Comment VARCHAR(50) COMMENT 'Freetext comment for market specific information if any',
CREATED_BY VARCHAR(255) COMMENT 'Name of the user' ,
CREATED_AT TIMESTAMP_NTZ(9) COMMENT 'Data ingestion timestamp'
);

/*
Creation of LOOKUP table
*/
CREATE TABLE EDI_EODP.PROD.EODP_LOOKUP(
Actflag CHAR(1) COMMENT 'Record Level Action Status',
Acttime DATE COMMENT 'Last Changed date of record at EDI',
TypeGroup CHAR(10) COMMENT 'Link between coded data and Combined Lookup Table',
Code CHAR(10) COMMENT 'Code data value',
Lookup VARCHAR(70) COMMENT 'Lookup value for Code (previous field)',
UNIQUE (TypeGroup, Code)
);

/*
Pre-requisite 4: Creating the File Format
Query to create a file format for EODP_EQ data. Both CSV and TAB separated files are considered as CSV file type. We specify '\t' as the delimiter, where '\t' represents a tab. The file typically has column headers as the first row, so we skip the first row during processing.
Column parser cannot be used simultaneously.

File format for EODP_EQ
*/
CREATE FILE FORMAT EDI_EODP.OPERATIONS.EODP_EQ_FILE_FORMAT
TYPE = $file_type
FIELD_DELIMITER = $field_delimiter
SKIP_HEADER = 2
SKIP_BLANK_LINES = TRUE
TIMESTAMP_FORMAT = $timestamp_format
DATE_FORMAT = $date_format
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
ENCODING = $encoding;

/*
Pre-requisite 5: Creating a External Stage
Creation of an External Stage for EODP_EQ data. External Stage is a temporary location in Snowflake where files will be stored temporarily before loading them into your landing table.

The stage references the storage integration and includes the file format.
*/
CREATE STAGE EDI_EODP.OPERATIONS.EODP_EQ_STAGE
STORAGE_INTEGRATION = aws_sf_integration
URL = $bucket_url
FILE_FORMAT = $file_format;


/*
Pre-requisite 6: Creating Snowflake Snowpipe with auto-ingest enabled
Query to create a Snowpipe for EODP_EQ data.

Using ACCOUNTADMIN role:
Creating a pipe involves configuring various settings related to data ingestion, including authentication, file format, location, and more. Therefore, the Account Admin role is required.
*/

USE ROLE ACCOUNTADMIN;

/*
Snowpipe for EODP_EQ:

The pipe retrieves data from the stage and loads it into the landing table.
The AUTO_INGEST=true parameter specifies to read event notifications sent from an S3 bucket to an SQS queue when new data is ready to load.
*/
CREATE PIPE EDI_EODP.OPERATIONS.EODP_EQ_SNOWPIPE
AUTO_INGEST = TRUE AS
COPY INTO EDI_EODP.OPERATIONS.EODP_EQ_INC_LANDING
FROM @EDI_EODP.OPERATIONS.EODP_EQ_STAGE
FILE_FORMAT = EDI_EODP.OPERATIONS.EODP_EQ_FILE_FORMAT;

/*
Pre-requisite 7: Creation of Stream
Queries to create a stream for EODP_EQ  on the landing table.

To monitor when the landing table receives new data from the files loaded in the S3 bucket, we create a stream on the EODP_EQ landing table.
*/

CREATE STREAM EDI_EODP.OPERATIONS.EODP_EQ_STREAM
ON TABLE EDI_EODP.OPERATIONS.EODP_EQ_INC_LANDING APPEND_ONLY = TRUE;

/*
Pre-requisite 8: Creation of Secure views

A view allows the result of a query to be accessed as if it were a table.
Query to create secure view on top of Final table
*/

CREATE SECURE VIEW EDI_EODP.PROD.EODP_EQ_INC_PRODUCT(
MIC COMMENT 'Swift`s ISO Standard 10383 Market Identification Code',
pricefilesymbol COMMENT 'Ticker/localcode',
Isin COMMENT 'ISIN ISO Standard 6166',
Currency COMMENT 'ISO Standard 4217 Code for Base Currency',
PriceDate COMMENT 'Real date of price - note that this is does not necessarily match the closedate implied in the file name',
Open COMMENT 'The first price of the trading day or session (can bederived,not necessarily the first trade/transaction)',
High COMMENT 'Day`s high price',
Low COMMENT 'Day`s low price',
Close COMMENT 'The official close of the trading day or session (can be derived,not necessarily the last trade/transaction)',
Mid COMMENT 'The average of the bid and ask prices rounded up to the price format allowed for that tradableinstrument/currency (London SE only)',
Ask COMMENT 'The selling price (ask/offer) for securities in the market',
Last COMMENT 'The most recent (last) trade/transaction price of the trading day or session',
Bid COMMENT 'the buying price for securities in the market',
Bidsize COMMENT 'Quantity of a security that investors are willing to purchase at a specified bid price',
Asksize COMMENT 'Quantity of a security that people are willing to sell at a specified ask price',
TradedVolume COMMENT 'The number of shares or contracts traded during thetrading day or session',
SecID COMMENT 'Security ID from EDI`s Worldwide Corporate Action Database to link into Corporate Actions',
MktCloseDate COMMENT 'Market Close Date',
VolFlag COMMENT 'Relates to trading Volume, A= Absolute, T= Stated in thousands trading day or session',
Issuername COMMENT 'EDI`s Masterfile Issuer name if link established',
SecTyCD COMMENT 'EDI`s Masterfile Security Type Code if link established. EQS (Equity), PRF (Preference), DR (Depositary Receipt)',
SecurityDesc COMMENT 'EDI`s Masterfile Security Description if link established',
Sedol COMMENT 'Sedol Code',
Uscode COMMENT 'Extracted from US an Canadian Isins',
PrimaryExchgCD COMMENT 'EDI proprietary Exchange Code of primary Listing if known',
ExchgCD COMMENT 'EDI proprietary Exchange Code',
TradedValue COMMENT 'The total value of shares or contracts traded during the trading day or session',
TotalTrades COMMENT 'Total numbers of trades in this stock',
Comment COMMENT 'Freetext comment for market specific information if any',
CREATED_BY COMMENT 'Name of the user' ,
CREATED_AT COMMENT 'Data ingestion timestamp'
)
AS
SELECT *
FROM EDI_EODP.PROD.EODP_EQ_INC
WHERE
((MktCloseDate > CURRENT_TIMESTAMP() - INTERVAL '1 MONTH') and 
MktCloseDate <= CURRENT_TIMESTAMP());

/*
Query to create secure view on top of Lookup table
*/
CREATE SECURE VIEW EDI_EODP.PROD.EODP_LOOKUP_PRODUCT(
    ACTFLAG COMMENT 'Record Level Action Status',
	ACTTIME COMMENT 'Last Changed date of record at EDI',
	TYPEGROUP COMMENT 'Link between coded data and Combined Lookup Table',
	CODE COMMENT 'Code data value',
	LOOKUP COMMENT 'Lookup value for Code (previous field)')
AS
SELECT *
FROM EDI_EODP.PROD.EODP_LOOKUP;