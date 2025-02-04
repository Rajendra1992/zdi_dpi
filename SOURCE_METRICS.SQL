CREATE PROCEDURE pmf.LoadTableDetails_sp
AS
BEGIN
    -- Clear the existing data in the TableDetails table
    TRUNCATE TABLE pmf.TableDetails;

    -- Insert the new data
    INSERT INTO pmf.TableDetails (DatabaseName, TableName, SchemaName, RowCounts, TotalSizeMB)
    SELECT 
        DB_NAME() AS DatabaseName,
        t.name AS TableName,
        s.name AS SchemaName,
        p.rows AS RowCounts,
        SUM(a.total_pages) * 8 / 1024 AS TotalSizeMB
    FROM 
        sys.tables AS t
    INNER JOIN 
        sys.schemas AS s ON t.schema_id = s.schema_id
    INNER JOIN 
        sys.partitions AS p ON t.object_id = p.object_id
    INNER JOIN 
        sys.allocation_units AS a ON p.partition_id = a.container_id
    WHERE 
        p.index_id IN (0, 1) -- 0 for heap tables, 1 for clustered index tables
    GROUP BY 
        t.name, s.name, p.rows
    ORDER BY 
        s.name, t.name;
END;
