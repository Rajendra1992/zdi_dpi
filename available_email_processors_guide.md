# Available Email Processors Guide

## 🔍 Common Email Processors in NiFi

### Standard Email Processors (Available in Most Versions):

1. **ConsumeIMAP** - Consumes emails from IMAP server
2. **GetEmail** - Gets emails from email server
3. **PutEmail** - Sends emails
4. **ListenSMTP** - Listens for incoming SMTP emails
5. **ListenEmail** - Listens for incoming emails

### Email Processing Processors:

1. **ExtractEmailAttachments** - Extracts attachments (may have different names)
2. **SplitEmail** - Splits email into parts (may not be available)
3. **ExtractEmailHeaders** - Extracts email headers
4. **ExtractEmailContent** - Extracts email content

## 🔍 Alternative Workflows Without ExtractEmailAttachments

### Workflow 1: Using GetEmail + RouteOnAttribute

```
GetEmail → RouteOnAttribute → ExtractContent → PutSnowflakeInternalStageFile → ExecuteSQL
```

#### GetEmail Configuration:
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

#### RouteOnAttribute Configuration:
```
Routing Strategy: Route to Property name
Property Name: csv_attachment
Property Value: ${email.attachments:contains('.csv'):or(${email.attachments:contains('.CSV')})}
```

### Workflow 2: Using ConsumeIMAP + RouteOnAttribute + ExtractContent

```
ConsumeIMAP → RouteOnAttribute → ExtractContent → PutSnowflakeInternalStageFile → ExecuteSQL
```

#### RouteOnAttribute Configuration:
```
Routing Strategy: Route to Property name
Property Name: csv_attachment
Property Value: ${filename:endsWith('.csv'):or(${filename:endsWith('.CSV')})}
```

### Workflow 3: Using GetEmail + ExtractContent + RouteOnAttribute

```
GetEmail → ExtractContent → RouteOnAttribute → PutSnowflakeInternalStageFile → ExecuteSQL
```

#### ExtractContent Configuration:
```
Extract Mode: Extract Content
```

#### RouteOnAttribute Configuration:
```
Routing Strategy: Route to Property name
Property Name: csv_content
Property Value: ${filename:endsWith('.csv'):or(${filename:endsWith('.CSV')})}
```

## 🔍 Check Available Processors

### Step 1: Search for Email Processors
1. In NiFi UI, go to processor palette
2. Search for these terms:
   - "email"
   - "mail"
   - "imap"
   - "smtp"
   - "attachment"
   - "extract"

### Step 2: List Available Processors
Please check and list which of these processors are available:

#### Email Consumption:
- [ ] ConsumeIMAP
- [ ] GetEmail
- [ ] ListenSMTP
- [ ] ListenEmail

#### Email Processing:
- [ ] ExtractEmailAttachments
- [ ] ExtractEmailContent
- [ ] ExtractEmailHeaders
- [ ] SplitEmail
- [ ] ExtractContent (standard processor)

#### Content Processing:
- [ ] ExtractContent
- [ ] RouteOnAttribute
- [ ] UpdateAttribute

## 🔍 Simple Alternative Workflow

### If Only Basic Processors Are Available:

```
ConsumeIMAP → RouteOnAttribute → PutSnowflakeInternalStageFile → ExecuteSQL
```

#### ConsumeIMAP Configuration:
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

#### RouteOnAttribute Configuration:
```
Routing Strategy: Route to Property name
Property Name: csv_email
Property Value: ${email.subject:contains('CSV'):or(${email.body:contains('.csv')})}
```

#### PutSnowflakeInternalStageFile Configuration:
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
```

## 🔍 Manual Email Processing

### If No Email Attachment Processors Are Available:

1. **Use ConsumeIMAP to get emails**
2. **Use RouteOnAttribute to filter emails with attachments**
3. **Use ExtractContent to extract attachment content**
4. **Use RouteOnAttribute again to filter CSV files**
5. **Upload to Snowflake**

### Step-by-Step Configuration:

#### Step 1: ConsumeIMAP
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

#### Step 2: RouteOnAttribute (Filter emails with attachments)
```
Routing Strategy: Route to Property name
Property Name: has_attachments
Property Value: ${email.attachments:isNotEmpty()}
```

#### Step 3: ExtractContent
```
Extract Mode: Extract Content
```

#### Step 4: RouteOnAttribute (Filter CSV files)
```
Routing Strategy: Route to Property name
Property Name: is_csv
Property Value: ${filename:endsWith('.csv'):or(${filename:endsWith('.CSV')})}
```

#### Step 5: PutSnowflakeInternalStageFile
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
```

## 🔍 Connection Flow

### Required Connections:
1. **ConsumeIMAP** → **RouteOnAttribute** (success)
2. **RouteOnAttribute** → **ExtractContent** (has_attachments)
3. **ExtractContent** → **RouteOnAttribute** (success)
4. **RouteOnAttribute** → **PutSnowflakeInternalStageFile** (is_csv)
5. **PutSnowflakeInternalStageFile** → **ExecuteSQL** (success)

### Auto-terminate Relationships:
- RouteOnAttribute: unmatched (for both instances)

## 🔍 Testing the Workflow

### Test Steps:
1. **Start ConsumeIMAP** - Verify it connects to Gmail
2. **Start RouteOnAttribute** - Check if it receives emails
3. **Start ExtractContent** - Verify it processes attachments
4. **Start second RouteOnAttribute** - Check CSV filtering
5. **Start PutSnowflakeInternalStageFile** - Verify Snowflake connection
6. **Start ExecuteSQL** - Check data loading

### Expected Flow:
1. ConsumeIMAP fetches emails from Gmail
2. RouteOnAttribute filters emails with attachments
3. ExtractContent extracts attachment content
4. RouteOnAttribute filters only CSV files
5. PutSnowflakeInternalStageFile uploads to Snowflake stage
6. ExecuteSQL loads data from stage to table

## 🔍 Troubleshooting

### Common Issues:
1. **No emails received** - Check Gmail credentials and IMAP settings
2. **No attachments found** - Check email structure and RouteOnAttribute logic
3. **CSV files not filtered** - Verify filename patterns and RouteOnAttribute expressions
4. **Snowflake connection failed** - Check credentials and network connectivity

### Debug Steps:
1. **Check processor logs** for error messages
2. **Verify email attributes** using LogAttribute processor
3. **Test expressions** using TestExpression processor
4. **Check file content** using LogContent processor