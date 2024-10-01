/*  
Queries related to External Stage

Check the files in the External Stage connected to the S3 bucket.
*/
LIST @EDI_EODP.OPERATIONS.EODP_FI_STAGE;

/*
Queries related to Snowpipe

Retrieve pipe details.*/
SHOW PIPES;
/*
Check the status of the pipe to retrieve the latest ingested file.
*/ 
SELECT SYSTEM$PIPE_STATUS('EDI_EODP.OPERATIONS.EODP_FI_SNOWPIPE');
/*
Refresh the pipe - Use this when we need to reload old data into the table.
**Only works for files uploaded to S3 within the past 7 days from the current date.**
*/ 
ALTER PIPE EDI_EODP.OPERATIONS.EODP_FI_SNOWPIPE REFRESH; 
/*
Queries on top of Landing Table

Check the copy history for the Landing table. This can be used to review the history of files copied into the landing table for last 24 hours, including any failed files.
Make sure to **Select the Database and Schema in the Worksheet** before executing.
*/  
SELECT *
FROM TABLE(EDI_EODP.INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'EDI_EODP.OPERATIONS.EODP_FI_INC_LANDING', START_TIME=> DATEADD(HOURS, -24, CURRENT_TIMESTAMP())));
/*
View the content of the Landing Table.
*/ 
SELECT * FROM EDI_EODP.OPERATIONS.EODP_FI_INC_LANDING;
/*
Queries related to Stream

Display all the data in a specified stream.
*/ 
SELECT * FROM EDI_EODP.OPERATIONS.EODP_FI_STREAM;
/* 
Queries related to Temp Table

View the content of the Temp Table.
*/ 
SELECT * FROM EDI_EODP.OPERATIONS.EODP_FI_INC_TEMP;
/* 
Queries Related to Final Table

View the content of the Prod Table.
*/ 
SELECT * FROM EDI_EODP.PROD.EODP_FI_INC;
/*
Queries Related to Error Table

View the content of the Error Table.
*/
SELECT * FROM EDI_EODP.OPERATIONS.EODP_FI_INC_ERROR;
/* 
Queries to check all the Tables in a specific Schema

List all the Tables in the Operations schema.
*/ 
SHOW TABLES IN EDI_EODP.OPERATIONS;
/* 
List all the Tables in the PROD schema.
*/
SHOW TABLES IN EDI_EODP.PROD;
/*
Queries related to file format

Display all the available file formats in a given Database and schema.
Make sure to **Select the Database and Schema in the Worksheet** before executing.
*/ 
SELECT * FROM EDI_EODP.INFORMATION_SCHEMA.FILE_FORMATS;
/*
Queries related to Task

Display all tasks.
*/ 
SHOW TASKS;
/* 
Describe a specific task.
*/ 
DESCRIBE TASK EDI_EODP.OPERATIONS.EODP_FI_PUSH_STREAM_TO_TEMP;

DESCRIBE TASK EDI_EODP.OPERATIONS.EODP_FI_PUSH_TO_EODP_FI_INC;
/*
Queries related to Stored Procedure

Call the stored procedure - Used to manually run the procedure for troubleshooting in case of errors or failures in the Task.
*/
CALL EDI_EODP.OPERATIONS.EODP_FI_SP();


/*
Queries related to Secure View on EODP_FI

Verify the view designed with specific conditions.
Executing a query to verify the secure view on EODP_FI table.
*/ 

-- Selecting all columns from the secure view 
SELECT * FROM EDI_EODP.PROD.EODP_FI_INC_PRODUCT 

 
/* 
Show all the parameters used in the schema. 
*/
SHOW PARAMETERS;