# Correct NiFi Processor Configurations

## 📧 PROCESSOR 1: ConsumeIMAP

### Properties Tab:
```
Host: imap.gmail.com
Port: 993
Username: skishore.sanchina@gmail.com
Password: your-gmail-app-password
Folder: INBOX
Fetch Size: 10
Delete Messages: false
Mark Messages as Read: true
Use SSL: true
Connection timeout: 30 sec
```

### Scheduling Tab:
```
Scheduling Strategy: TIMER_DRIVEN
Run Schedule: 30 sec
Concurrent Tasks: 1
```

## 📎 PROCESSOR 2: ExtractEmailAttachments

### Properties Tab:
```
Attachment Filter: .*\.csv$
Extract Attachments Only: true
Include Original Message: false
Attachment Name Pattern: .*
Attachment Size Limit: 100 MB
```

### Settings Tab:
```
Auto-terminate relationships: ☑️ original
```

**IMPORTANT**: If you're still getting property validation errors, try these alternative property names:

**Alternative 1 (for newer NiFi versions):**
```
attachment-filter: .*\.csv$
extract-attachments-only: true
include-original-message: false
```

**Alternative 2 (for older NiFi versions):**
```
AttachmentFilter: .*\.csv$
ExtractAttachmentsOnly: true
IncludeOriginalMessage: false
```

## 🔀 PROCESSOR 3: RouteOnAttribute

### Properties Tab:
```
Routing Strategy: Route to Property name
```

**Add Custom Property:**
```
Property Name: valid_csv
Property Value: ${filename:endsWith('.csv')}
```

### Settings Tab:
```
Auto-terminate relationships: ☑️ unmatched
```

**IMPORTANT**: For RouteOnAttribute, you need to:
1. Add the custom property correctly
2. Connect the relationships to downstream processors
3. Auto-terminate unused relationships

## ❄️ PROCESSOR 4: PutSnowflakeInternalStageFile

### Properties Tab:
```
Account: your-account.snowflakecomputing.com
Username: your-snowflake-username
Password: your-snowflake-password
Database: DEMO_DB
Schema: PUBLIC
Warehouse: COMPUTE_WH
Role: SYSADMIN
Authentication Type: Username/Password
Stage Name: @%EMPLOYEE_DATA
Connection Timeout: 30 sec
Max Wait Time: 300 sec
```

### Scheduling Tab:
```
Scheduling Strategy: EVENT_DRIVEN
Concurrent Tasks: 2
```

## 📊 PROCESSOR 5: ExecuteSQL

### Properties Tab:
```
Database Connection Pooling Service: (Create Snowflake connection pool)
SQL select query:
COPY INTO DEMO_DB.PUBLIC.EMPLOYEE_DATA 
FROM @%EMPLOYEE_DATA 
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_DELIMITER = ',')
PURGE = TRUE;
```

### Scheduling Tab:
```
Scheduling Strategy: EVENT_DRIVEN
Concurrent Tasks: 1
```

## 🔗 PROCESSOR CONNECTIONS

### Required Connections:
1. **ConsumeIMAP** → **ExtractEmailAttachments** (success)
2. **ExtractEmailAttachments** → **RouteOnAttribute** (attachment)
3. **RouteOnAttribute** → **PutSnowflakeInternalStageFile** (valid_csv)
4. **PutSnowflakeInternalStageFile** → **ExecuteSQL** (success)

### Auto-terminate Relationships:
- ExtractEmailAttachments: original
- RouteOnAttribute: unmatched

## 🛠️ TROUBLESHOOTING SPECIFIC ERRORS

### ExtractEmailAttachments Errors:

**Error**: "Extract Attachments Only' validated against 'true' is invalid"

**Solutions**:
1. Try different property names based on your NiFi version
2. Delete and recreate the processor
3. Configure properties one by one

**Step-by-step fix**:
1. Delete the existing ExtractEmailAttachments processor
2. Drag a new ExtractEmailAttachments processor to canvas
3. Right-click → Configure
4. Go to Properties tab
5. Add properties manually:
   - Click the "+" button to add property
   - Enter property name exactly as shown above
   - Enter property value
6. Go to Settings tab
7. Check "Auto-terminate relationships" for "original"

### RouteOnAttribute Errors:

**Error**: "No Expressions found" and "Relationship not connected"

**Solutions**:
1. Ensure the expression syntax is correct
2. Connect all relationships to downstream processors
3. Auto-terminate unused relationships

**Step-by-step fix**:
1. Right-click RouteOnAttribute → Configure
2. Go to Properties tab
3. Set "Routing Strategy" to "Route to Property name"
4. Click "+" to add custom property:
   - Property Name: `valid_csv`
   - Property Value: `${filename:endsWith('.csv')}`
5. Go to Settings tab
6. Check "Auto-terminate relationships" for "unmatched"
7. Connect the "valid_csv" relationship to PutSnowflakeInternalStageFile

## 🔧 ALTERNATIVE CONFIGURATIONS

### If ExtractEmailAttachments still doesn't work:

**Try this minimal configuration**:
```
Attachment Filter: .*\.csv$
Extract Attachments Only: true
```

**Or try without the filter**:
```
Extract Attachments Only: true
```

### If RouteOnAttribute still doesn't work:

**Try this alternative expression**:
```
Property Name: valid_csv
Property Value: ${filename:contains('.csv')}
```

**Or use a simpler approach**:
```
Property Name: valid_csv
Property Value: true
```

## ✅ VALIDATION CHECKLIST

Before starting processors, verify:

- [ ] All processors are properly connected
- [ ] No validation errors in any processor
- [ ] Auto-terminate relationships are configured
- [ ] Property names match your NiFi version
- [ ] Expressions are syntactically correct
- [ ] Upstream connections exist for all processors

## 🚀 STARTUP ORDER

1. Start ConsumeIMAP first
2. Start ExtractEmailAttachments
3. Start RouteOnAttribute
4. Start PutSnowflakeInternalStageFile
5. Start ExecuteSQL

## 📝 NOTES

- Property names are case-sensitive
- Different NiFi versions may use different property naming conventions
- Always check processor logs for detailed error messages
- Ensure all required controller services are enabled
- Test connections before starting processors