/*
Pre-requisite 1: Need to execute the EDI_EODP_FI_SETUP.sql and EDI_EODP_FI_TRANSFORMATION.sql file before executing this.
Pre-requisite 2: Define variables for specifying snowflake warehouse, and scheduled time.
*/

SET snowflake_warehouse = 'EDI_EODP_WH';
SET schedule_time = '720 MINUTES';
/*
Creating Tasks

Task 1: Query to create a task that copies the stream data into Temp table. A 720-minute buffer time is given to avoid flushing of the stream and prevent multiple pushes of the same data.
Task creation for EODP_FI.
*/ 
CREATE TASK EDI_EODP.OPERATIONS.EODP_FI_PUSH_STREAM_TO_TEMP
 WAREHOUSE = $snowflake_warehouse
 SCHEDULE = $schedule_time
WHEN 
 SYSTEM$STREAM_HAS_DATA('EDI_EODP.OPERATIONS.EODP_FI_STREAM')
 AS INSERT INTO EDI_EODP.OPERATIONS.EODP_FI_INC_TEMP SELECT * FROM EDI_EODP.OPERATIONS.EODP_FI_STREAM;
/*
Task 2: Creating a second task to process the data inside the Temp table into the final table using a stored procedure.
*/
CREATE TASK EDI_EODP.OPERATIONS.EODP_FI_PUSH_TO_EODP_FI_INC
 WAREHOUSE = $snowflake_warehouse
 AFTER EDI_EODP.OPERATIONS.EODP_FI_PUSH_STREAM_TO_TEMP
 AS CALL EDI_EODP.OPERATIONS.EODP_FI_SP();
/* 
Query to specify Snowflake to utilize the Accountadmin role to start the tasks.
*/ 
USE ROLE ACCOUNTADMIN;
/*
Query to initiate the tasks
By default, the task state is suspended.
*/ 
ALTER TASK EDI_EODP.OPERATIONS.EODP_FI_PUSH_TO_EODP_FI_INC RESUME;

ALTER TASK EDI_EODP.OPERATIONS.EODP_FI_PUSH_STREAM_TO_TEMP RESUME;