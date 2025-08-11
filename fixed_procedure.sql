CREATE OR REPLACE PROCEDURE DEV_DB_MANAGER.MASKING.TRANSFER_CLASSIFICATION_DETAILS("DB_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "TABLE_NAME" VARCHAR, "CLASSIFICATION_OWNER" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    -- Conditional delete based on table_name using CASE statement
    -- Delete records for specific DATABASE, SCHEMA, and CLASSIFICATION_OWNER combination
    EXECUTE IMMEDIATE 
    CASE 
        WHEN table_name = ''DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS'' THEN 
            ''DELETE FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
            WHERE DATABASE = '''''' || db_name || ''''''
              AND SCHEMA = '''''' || schema_name || ''''''
              AND CLASSIFICATION_OWNER = '''''' || classification_owner || '''''';''
        ELSE 
            ''SELECT 1;'' 
    END;

    -- Insert new records with IS_ACTIVE = TRUE
    -- Fetch records only for the specified CLASSIFICATION_OWNER
    EXECUTE IMMEDIATE ''
    INSERT INTO DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS (
        DATE,
        DATABASE,
        SCHEMA,
        "TABLE",
        "COLUMN",
        CLASSIFICATION,
        TAG,
        IS_ACTIVE,
        CLASSIFICATION_OWNER
    )
    SELECT 
        DATE,
        DATABASE,
        SCHEMA,
        "TABLE",
        "COLUMN",
        CLASSIFICATION,
        TAG,
        '' || 
        CASE 
            WHEN table_name = ''''ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS'''' THEN ''''TRUE'''' 
            ELSE ''''IS_ACTIVE'''' 
        END || '' AS IS_ACTIVE,
        '' || 
        CASE 
            WHEN table_name = ''''ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS'''' THEN ''''''''ALTR'''''''' 
            ELSE ''''CLASSIFICATION_OWNER'''' 
        END || '' AS CLASSIFICATION_OWNER
    FROM (
        SELECT 
            DATE,
            DATABASE,
            SCHEMA,
            "TABLE",
            "COLUMN",
            '' || 
            CASE 
                WHEN table_name = ''''ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS'''' THEN ''''GDLP_CLASSIFICATION'''' 
                ELSE ''''CLASSIFICATION'''' 
            END || '' AS CLASSIFICATION,
            '' || 
            CASE 
                WHEN table_name = ''''ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS'''' THEN ''''MAPPED_TAG'''' 
                ELSE ''''TAG'''' 
            END || '' AS TAG,
            '' || 
            CASE 
                WHEN table_name = ''''ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS'''' THEN ''''TRUE'''' 
                ELSE ''''IS_ACTIVE'''' 
            END || '' AS IS_ACTIVE,
            '' || 
            CASE 
                WHEN table_name = ''''ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS'''' THEN ''''''''ALTR'''''''' 
                ELSE ''''CLASSIFICATION_OWNER'''' 
            END || '' AS CLASSIFICATION_OWNER,
            ROW_NUMBER() OVER (
                PARTITION BY DATABASE, SCHEMA, "TABLE", "COLUMN" 
                ORDER BY DATE DESC
            ) AS rn
        FROM '' || table_name || ''
        WHERE DATABASE = '''''' || db_name || ''''''
          AND SCHEMA = '''''' || schema_name || ''''''
          AND IS_ACTIVE = TRUE'' ||
          CASE 
              WHEN table_name = ''ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS'' THEN 
                  '' AND MAPPED_TAG != ''''NO MAPPING''''''
              ELSE 
                  '' AND TAG IS NOT NULL AND CLASSIFICATION_OWNER = '''''' || classification_owner || '''''''
          END || ''
    ) src
    WHERE src.rn = 1 -- Keep only the latest record for each (DATABASE, SCHEMA, TABLE, COLUMN)
      AND NOT EXISTS (
        SELECT 1
        FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS tgt
        WHERE src.DATABASE = tgt.DATABASE
          AND src.SCHEMA = tgt.SCHEMA
          AND src."TABLE" = tgt."TABLE"
          AND src."COLUMN" = tgt."COLUMN"
          AND src.CLASSIFICATION = tgt.CLASSIFICATION
          AND src.TAG = tgt.TAG
          AND src.CLASSIFICATION_OWNER = tgt.CLASSIFICATION_OWNER
    )
    '';

    RETURN ''Data transfer completed successfully from table: '' || table_name || '' for classification_owner: '' || classification_owner;
END;
';