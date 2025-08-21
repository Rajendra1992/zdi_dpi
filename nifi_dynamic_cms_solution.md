# NiFi Solution: Dynamic CMS NPPES File Download

## The Problem
The expression `${now():format('MMMM_dd_yyyy')}` doesn't work because:
1. CMS doesn't release files daily
2. Files are released on specific dates (not always the current date)
3. File naming follows patterns like: `NPPES_Data_Dissemination_January_08_2025.zip`

## Solutions

### Solution 1: Directory Listing Approach (Recommended)

This approach scrapes the CMS directory to find the latest available file.

#### Step 1: Add InvokeHTTP for Directory Listing
```
Processor: InvokeHTTP
Properties:
- HTTP Method: GET
- Remote URL: https://download.cms.gov/nppes/NPI_Files.html
- Follow Redirects: true
```

#### Step 2: Add ExtractText to Parse HTML
```
Processor: ExtractText
Properties:
- html.links: (?i)href="(NPPES_Data_Dissemination_[^"]+\.zip)"
- Include Capture Group 0: false
```

#### Step 3: Add EvaluateJsonPath or UpdateAttribute
```
Processor: UpdateAttribute
Properties:
- latest.file.url: https://download.cms.gov/nppes/${html.links}
```

#### Step 4: Add Second InvokeHTTP for File Download
```
Processor: InvokeHTTP
Properties:
- HTTP Method: GET
- Remote URL: ${latest.file.url}
- Follow Redirects: true
```

### Solution 2: Multiple URL Attempts (Fallback Approach)

Try multiple possible URLs based on recent dates.

#### Updated InvokeHTTP Configuration:
```
Processor: GenerateFlowFile (to create test dates)
Properties:
- Custom Text: dummy

Processor: UpdateAttribute
Properties:
- date.today: ${now():format('MMMM_dd_yyyy')}
- date.yesterday: ${now():minus(86400000):format('MMMM_dd_yyyy')}
- date.week.ago: ${now():minus(604800000):format('MMMM_dd_yyyy')}
- date.month.start: ${now():format('MMMM_01_yyyy')}
- url.attempt.1: https://download.cms.gov/nppes/NPPES_Data_Dissemination_${date.today}.zip
- url.attempt.2: https://download.cms.gov/nppes/NPPES_Data_Dissemination_${date.yesterday}.zip
- url.attempt.3: https://download.cms.gov/nppes/NPPES_Data_Dissemination_${date.week.ago}.zip
- url.attempt.4: https://download.cms.gov/nppes/NPPES_Data_Dissemination_${date.month.start}.zip
```

### Solution 3: Fixed Monthly Pattern (Simplest)

Use a more predictable pattern based on monthly releases.

#### Updated Expression:
```
For monthly files (typically released on 8th-15th of each month):
https://download.cms.gov/nppes/NPPES_Data_Dissemination_${now():format('MMMM')}_08_${now():format('yyyy')}.zip

Alternative attempts:
- ${now():format('MMMM')}_08_${now():format('yyyy')}
- ${now():format('MMMM')}_14_${now():format('yyyy')}
- ${now():format('MMMM')}_15_${now():format('yyyy')}
```

## Complete NiFi Flow Implementation

### Method 1: Directory Parsing Flow

```
1. GenerateFlowFile (trigger)
   ↓
2. InvokeHTTP (get directory listing)
   URL: https://download.cms.gov/nppes/NPI_Files.html
   ↓
3. ExtractText (parse HTML for file links)
   Regex: href="(NPPES_Data_Dissemination_[^"]+\.zip)"
   ↓
4. UpdateAttribute (build download URL)
   file.url: https://download.cms.gov/nppes/${html.links.1}
   ↓
5. InvokeHTTP (download actual file)
   URL: ${file.url}
   ↓
6. PutS3Object (upload to S3)
```

### Method 2: Multiple Attempts Flow

```
1. GenerateFlowFile
   ↓
2. UpdateAttribute (create multiple URL attempts)
   ↓
3. InvokeHTTP (try first URL)
   ↓ (on failure)
4. UpdateAttribute (switch to next URL)
   ↓
5. InvokeHTTP (try second URL)
   ↓ (continue pattern)
6. PutS3Object (on success)
```

## Detailed Processor Configurations

### ExtractText Processor Configuration
```
Property Name: html.links
Property Value: href="(NPPES_Data_Dissemination_[A-Za-z]+_\d+_\d{4}\.zip)"
Character Set: UTF-8
Maximum Buffer Size: 1 MB
Include Capture Group 0: false
```

### UpdateAttribute for URL Building
```
Property Name: download.url
Property Value: https://download.cms.gov/nppes/${html.links.1:urlEncode()}
```

### RouteOnAttribute for Error Handling
```
Property Name: has.file.url
Property Value: ${html.links.1:isEmpty():not()}
```

## Error Handling Strategy

### Add RouteOnAttribute after ExtractText:
```
Processor: RouteOnAttribute
Properties:
- file.found: ${html.links.1:isEmpty():not()}
- Routing Strategy: Route to Property name
```

### Connect relationships:
- `file.found` → Continue to download
- `unmatched` → Log error or send notification

## Testing the Expression

### Test UpdateAttribute Configuration:
```
Property Name: test.date.format
Property Value: ${now():format('MMMM_dd_yyyy')}

Property Name: test.url
Property Value: https://download.cms.gov/nppes/NPPES_Data_Dissemination_${now():format('MMMM_dd_yyyy')}.zip
```

### Check the generated values in NiFi:
1. Right-click UpdateAttribute processor
2. Select "View Data Provenance"
3. Check the attribute values in the FlowFile

## Alternative Date Formats to Try

```
Current format: ${now():format('MMMM_dd_yyyy')}
Result: January_15_2025

Try these alternatives:
1. ${now():format('MMMM_d_yyyy')} → January_8_2025 (single digit day)
2. ${now():format('MMM_dd_yyyy')} → Jan_15_2025 (short month)
3. ${now():format('MMMM_dd_yy')} → January_15_25 (2-digit year)
```

## Monthly Release Pattern Analysis

Based on CMS patterns, try these specific dates:
```
- First Monday of month: ${now():format('MMMM')}_${now():weekOfMonth():multiply(7):minus(${now():format('u')}):plus(2):format('dd')}_${now():format('yyyy')}
- 8th of month: ${now():format('MMMM')}_08_${now():format('yyyy')}
- 14th of month: ${now():format('MMMM')}_14_${now():format('yyyy')}
```

## Recommended Implementation

Use **Solution 1 (Directory Listing)** because:
1. Always gets the actual available file
2. Handles CMS's irregular release schedule
3. More reliable than guessing dates
4. Can identify both monthly and weekly files

The directory parsing approach is the most robust solution for handling the unpredictable file release patterns from CMS.