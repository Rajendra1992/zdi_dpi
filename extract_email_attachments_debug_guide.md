# ExtractEmailAttachments Debug Guide

## 🔍 Step 1: Identify Your NiFi Version

First, let's identify your NiFi version to determine the correct property names:

1. **Check NiFi Version:**
   - Go to NiFi UI
   - Look at the bottom of the page for version number
   - Or check the browser title bar

2. **Common NiFi Versions:**
   - NiFi 1.x (1.0 - 1.19)
   - NiFi 2.x (2.0 - 2.1)
   - NiFi 3.x (3.0+)

## 🔍 Step 2: Check Available Properties

### Method 1: Use NiFi UI to Discover Properties

1. **Add ExtractEmailAttachments Processor:**
   - Drag ExtractEmailAttachments to canvas
   - Right-click → Configure
   - Go to Properties tab
   - Look at the available properties in the dropdown

2. **Check Property Names:**
   - Click the "+" button to add a property
   - Look at the property name suggestions
   - Note down the exact property names

### Method 2: Check Processor Documentation

1. **In NiFi UI:**
   - Right-click on ExtractEmailAttachments
   - Select "Usage" or "Documentation"
   - Look for property names

2. **Check Processor Details:**
   - Right-click → Configure
   - Look at the "Details" tab
   - Check for property descriptions

## 🔍 Step 3: Alternative Processor Names

The processor might have a different name in your NiFi version:

### Try These Processor Names:
1. `ExtractEmailAttachments`
2. `ExtractEmailAttachment`
3. `EmailAttachmentExtractor`
4. `ExtractAttachments`
5. `EmailAttachmentsExtractor`

### Search for Email Attachment Processors:
1. In the processor palette, search for:
   - "email"
   - "attachment"
   - "extract"
   - "mail"

## 🔍 Step 4: Manual Property Discovery

### Step-by-Step Process:

1. **Add the Processor:**
   ```
   Drag ExtractEmailAttachments to canvas
   ```

2. **Configure Without Properties:**
   ```
   Right-click → Configure
   Don't add any properties yet
   Check if processor validates
   ```

3. **Add Properties One by One:**
   ```
   Click "+" to add property
   Try these names one by one:
   ```

### Try These Property Names:

#### Group 1: Filter Properties
```
filter
attachment-filter
AttachmentFilter
attachmentFilter
file-filter
FileFilter
pattern
Pattern
regex
Regex
```

#### Group 2: Extract Properties
```
extract
extract-only
extractOnly
extract_attachments_only
extractAttachmentsOnly
attachments-only
attachmentsOnly
extractAttachments
```

#### Group 3: Include Properties
```
include
include-original
includeOriginal
include_original
original
includeOriginalMessage
```

#### Group 4: Size Properties
```
size
size-limit
sizeLimit
max-size
maxSize
limit
```

## 🔍 Step 5: Minimal Configuration Test

### Test 1: No Properties
```
1. Add ExtractEmailAttachments
2. Don't add any properties
3. Check if it validates
4. If yes, processor works without properties
```

### Test 2: Basic Properties
```
1. Add ExtractEmailAttachments
2. Add only one property at a time
3. Test each property name
4. Note which ones work
```

## 🔍 Step 6: Alternative Approaches

### If ExtractEmailAttachments Doesn't Work:

#### Option 1: Use SplitEmail Processor
```
1. Add SplitEmail processor
2. Configure to split attachments
3. Use RouteOnAttribute to filter CSV files
```

#### Option 2: Use GetEmail + RouteOnAttribute
```
1. Use GetEmail processor
2. Add RouteOnAttribute to filter attachments
3. Use ExtractContent for CSV files
```

#### Option 3: Use ConsumeIMAP + RouteOnAttribute
```
1. Use ConsumeIMAP processor
2. Add RouteOnAttribute to filter CSV attachments
3. Use ExtractContent processor
```

## 🔍 Step 7: Debug Commands

### Check Processor Availability:
```
1. In NiFi UI, go to processor palette
2. Search for "email"
3. Look for attachment-related processors
4. Note all available email processors
```

### Check Processor Logs:
```
1. Right-click on processor
2. Select "View State" or "View Logs"
3. Look for error messages
4. Check for property validation errors
```

## 🔍 Step 8: Fallback Configuration

### If Nothing Works, Use This Workflow:

```
ConsumeIMAP → RouteOnAttribute → ExtractContent → PutSnowflakeInternalStageFile
```

#### RouteOnAttribute Configuration:
```
Routing Strategy: Route to Property name
Property Name: csv_attachment
Property Value: ${filename:endsWith('.csv')}
```

#### ExtractContent Configuration:
```
Extract Mode: Extract Content
```

## 🔍 Step 9: Property Validation Test

### Test Each Property Name:

1. **For each property name below, try adding it:**

```
Property Names to Test:
- filter
- attachment-filter
- AttachmentFilter
- extract-only
- extractOnly
- extract_attachments_only
- include-original
- includeOriginal
- size-limit
- maxSize
- pattern
- regex
- file-filter
- attachments-only
- extractAttachments
- includeOriginalMessage
```

2. **For each property, try these values:**
```
Values to Test:
- .*\.csv$
- .*\.(csv|CSV)$
- .*csv.*
- true
- false
- 100 MB
- 100MB
```

## 🔍 Step 10: Report Results

After testing, please report:

1. **Your NiFi version:**
2. **Available property names:**
3. **Working property names:**
4. **Error messages received:**
5. **Alternative processors found:**

This will help provide the exact configuration for your NiFi version.