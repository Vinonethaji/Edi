/*
Pre-requisite 1: Need to execute the EDI_EODP_IX_SETUP.sql file before executing this.
Select the EDI_EODP_WH warehouse.
*/
USE WAREHOUSE EDI_EODP_WH;

/*
Creating a Stored Procedure
Query to create a Stored Procedure for EODP_IX.
Language used: Python
The procedure standardizes all columns with the feed file format, particularly checking and standardizing the DATE column. 
Since the procedure handles data from Temp table, we need to remove columns like METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID, and add CREATED_BY and CREATED_AT columns.
*/ 

CREATE PROCEDURE EDI_EODP.OPERATIONS.EODP_IX_SP()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'eodp_ix_sp'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, current_timestamp, lit

def eodp_ix_sp(session):

    # Variable Declaration.  
    tempTableName = 'EDI_EODP.OPERATIONS.EODP_IX_INC_TEMP'
    finalTableName = 'EDI_EODP.PROD.EODP_IX_INC'
    errorTableName = 'EDI_EODP.OPERATIONS.EODP_IX_INC_ERROR'
    metadataCol = ['METADATA$ACTION','METADATA$ISUPDATE','METADATA$ROW_ID']
    columns_with_date = ['PRICEDATE', 'MKTCLOSEDATE']
    created_by = 'CREATED_BY'
    created_by_value = 'SNOWPIPE_EDI'
    created_at = 'CREATED_AT'
    try:
        # Conversion of all values in SEDOL to Null
        update_query = f"UPDATE {tempTableName} SET SEDOL = NULL"
        session.sql(update_query).collect()

        # Conversion of PriceFileSymbol into empty string from Null
        null_update_statement = f"UPDATE {tempTableName} SET pricefilesymbol = CASE WHEN pricefilesymbol IS NULL THEN '' ELSE pricefilesymbol END"
        null_update_result = session.sql(null_update_statement).collect()
        
        # Conversion of date into expected format
        for column in columns_with_date:
            date_update_statement = f"UPDATE {tempTableName} SET {column} = REPLACE ({column}, '/', '-')"
            date_update_result = session.sql(date_update_statement).collect()
        # Get the Temp table into dataframe
        sql_statement = f'SELECT * FROM {tempTableName}'
        landing_df = session.sql(sql_statement)
        
        # Removing the Last Row to ignore EDI_ENDOFFILE
        end_of_file_df = landing_df.filter(col("MIC") != 'EDI_ENDOFFILE')  
        
        # Creating Created_at and Created_by metadata columns
        landing_df = end_of_file_df.with_column(f'{created_by}',lit(f'{created_by_value}'))
        landing_df = landing_df.with_column(f'{created_at}',current_timestamp())
        
        # Removing other metadatas from stream
        final_df = landing_df.drop(metadataCol[0],metadataCol[1],metadataCol[2])
            
        # Write data to final table
        final_df.write.mode("append").save_as_table(f'{finalTableName}')
    
        # Truncating the temp table
        temp_trunc_query =  f'TRUNCATE TABLE {tempTableName}'
        temp_trunc_res = session.sql(temp_trunc_query).collect()
    
        return f"INSERTED NEW RECORDS FROM S3 to EODP_IX TABLE SUCCESSFULLY.";

    except Exception as e:

        # Get the Temp table into dataframe
        sql_statement = f'SELECT * FROM {tempTableName}'
        landing_df = session.sql(sql_statement)
        
        # Creating Created at and created by columns
        landing_df = landing_df.with_column(f'{created_by}',lit(f'{created_by_value}'))
        final_df = landing_df.with_column(f'{created_at}',current_timestamp())

        # Write the data into error table
        final_df.write.mode("append").save_as_table(f'{errorTableName}')

        # Truncating the temp table
        temp_trunc_query =  f'TRUNCATE TABLE {tempTableName}'
        temp_trunc_res = session.sql(temp_trunc_query).collect()
        raise Exception(e)
$$;
