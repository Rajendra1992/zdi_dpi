# NiFi Dynamic URL Extraction - Complete Implementation Guide

## How Directory Listing Works Dynamically

The directory listing approach works by:
1. **Fetching the HTML page** that lists all available files
2. **Parsing the HTML** to extract actual file names that exist
3. **Building download URLs** using the extracted file names
4. **Downloading the files** using the dynamically built URLs

## Step-by-Step Implementation

### Step 1: Create GenerateFlowFile (Trigger)
```
Processor: GenerateFlowFile
Purpose: Starts the flow (acts as trigger)
Properties:
- File Size: 0B
- Batch Size: 1
- Data Format: Text
- Custom Text: trigger
Scheduling:
- Run Schedule: 0 0 1 * * ? (monthly on 1st day)
```

### Step 2: InvokeHTTP - Get Directory Listing
```
Processor: InvokeHTTP
Purpose: Fetch the HTML page containing file listings
Properties:
- HTTP Method: GET
- Remote URL: https://download.cms.gov/nppes/NPI_Files.html
- Follow Redirects: true
- Connection Timeout: 30 sec
- Read Timeout: 60 sec
- Include Date Header: true
- Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8
- User-Agent: Mozilla/5.0 (compatible; NiFi)
```

**What this does:** Downloads the HTML page that contains links to all available ZIP files.

### Step 3: ExtractText - Parse HTML for File Links
```
Processor: ExtractText
Purpose: Extract actual file names from HTML
Properties:
- Character Set: UTF-8
- Maximum Buffer Size: 1 MB
- Include Capture Group 0: false

Dynamic Properties to Add:
Property Name: monthly.file
Property Value: href="(NPPES_Data_Dissemination_(?:January|February|March|April|May|June|July|August|September|October|November|December)_\d{1,2}_\d{4}\.zip)"

Property Name: weekly.file  
Property Value: href="(NPPES_Data_Dissemination_Weekly_[^"]+\.zip)"

Property Name: latest.monthly
Property Value: href="(NPPES_Data_Dissemination_${now():format('MMMM')}_\d{1,2}_${now():format('yyyy')}\.zip)"
```

**How this works:**
- The regex `href="(NPPES_Data_Dissemination_January_\d{1,2}_2025\.zip)"` will find links like:
  - `href="NPPES_Data_Dissemination_January_08_2025.zip"`
  - `href="NPPES_Data_Dissemination_January_14_2025.zip"`
- It captures the actual filename that exists on the server
- The `\d{1,2}` matches any day (1-31)

### Step 4: UpdateAttribute - Build Download URLs
```
Processor: UpdateAttribute
Purpose: Create the complete download URLs
Properties:
- monthly.download.url: https://download.cms.gov/nppes/${monthly.file.1}
- weekly.download.url: https://download.cms.gov/nppes/${weekly.file.1}
- latest.download.url: https://download.cms.gov/nppes/${latest.monthly.1}
- selected.url: ${latest.download.url:isEmpty():ifElse(${monthly.download.url}, ${latest.download.url})}
```

**What this creates:**
- If `latest.monthly.1` = `NPPES_Data_Dissemination_January_14_2025.zip`
- Then `latest.download.url` = `https://download.cms.gov/nppes/NPPES_Data_Dissemination_January_14_2025.zip`

### Step 5: RouteOnAttribute - Check if URL Found
```
Processor: RouteOnAttribute
Purpose: Route based on whether file URL was found
Properties:
- Routing Strategy: Route to Property name
- url.found: ${selected.url:contains('NPPES_Data_Dissemination')}
- no.url.found: ${selected.url:contains('NPPES_Data_Dissemination'):not()}
```

### Step 6: InvokeHTTP - Download the File
```
Processor: InvokeHTTP (Second one)
Purpose: Download the actual file using dynamic URL
Properties:
- HTTP Method: GET
- Remote URL: ${selected.url}
- Follow Redirects: true
- Connection Timeout: 60 sec
- Read Timeout: 600 sec (10 minutes for large files)
- Send Body: false
```

**This uses the dynamically extracted URL to download the actual file.**

### Step 7: LogAttribute - Debug Information
```
Processor: LogAttribute
Purpose: Log the extracted information for debugging
Properties:
- Log Level: INFO
- Log Payload: false
- Attributes to Log: monthly.file.1,weekly.file.1,latest.monthly.1,selected.url
- Log prefix: "Dynamic URL Extraction: "
```

## Complete Flow Connections

```
GenerateFlowFile 
    ↓ (success)
InvokeHTTP (get directory)
    ↓ (Response)
ExtractText (parse HTML)
    ↓ (success)
UpdateAttribute (build URLs)
    ↓ (success)
RouteOnAttribute (check URL found)
    ↓ (url.found)
InvokeHTTP (download file)
    ↓ (Response)
PutS3Object (upload to S3)
    ↓ (success)
LogAttribute (success log)

RouteOnAttribute
    ↓ (no.url.found)
LogAttribute (error log)
```

## Example of How It Works Dynamically

### Input HTML (from CMS website):
```html
<a href="NPPES_Data_Dissemination_January_08_2025.zip">January 2025 File</a>
<a href="NPPES_Data_Dissemination_January_14_2025.zip">January 2025 Updated</a>
<a href="NPPES_Data_Dissemination_December_15_2024.zip">December 2024 File</a>
```

### ExtractText Results:
- `monthly.file.1` = `NPPES_Data_Dissemination_January_14_2025.zip`
- `latest.monthly.1` = `NPPES_Data_Dissemination_January_14_2025.zip`

### UpdateAttribute Results:
- `latest.download.url` = `https://download.cms.gov/nppes/NPPES_Data_Dissemination_January_14_2025.zip`

### Final Download:
InvokeHTTP downloads from the dynamically built URL.

## Advanced Regex Patterns

### For Current Month Only:
```regex
href="(NPPES_Data_Dissemination_${now():format('MMMM')}_\d{1,2}_${now():format('yyyy')}\.zip)"
```

### For Latest File (Any Month):
```regex
href="(NPPES_Data_Dissemination_\w+_\d{1,2}_\d{4}\.zip)"
```

### For Multiple File Types:
```regex
href="(NPPES_(?:Data_Dissemination|Deactivated_NPI_Report)_[^"]+\.zip)"
```

## Troubleshooting Steps

### Step 1: Test Directory Listing
Add a LogAttribute after the first InvokeHTTP to see the HTML content:
```
Properties:
- Log Level: DEBUG
- Log Payload: true
- Log prefix: "HTML Content: "
```

### Step 2: Test Regex Extraction
Add a LogAttribute after ExtractText:
```
Properties:
- Attributes to Log: monthly.file.1,monthly.file.2,monthly.file.3
- Log prefix: "Extracted Files: "
```

### Step 3: Test URL Building
Add a LogAttribute after UpdateAttribute:
```
Properties:
- Attributes to Log: latest.download.url,selected.url
- Log prefix: "Built URLs: "
```

## Why This Approach Works

1. **Always Current**: Gets actual files that exist on the server
2. **Handles Date Variations**: Works regardless of release day (8th, 14th, 15th)
3. **Multiple Options**: Can extract multiple files and choose the best one
4. **Error Handling**: Can detect when no files are found
5. **Flexible**: Regex can be adjusted for different file patterns

This approach dynamically discovers what files are actually available and builds the download URLs accordingly, eliminating the guesswork of predicting file names.