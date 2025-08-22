# ExtractEmailAttachments Processor Troubleshooting Guide

## Error Analysis

The errors you're encountering are:

1. **Property Validation Errors**: Properties not recognized by the processor
2. **Missing Upstream Connection**: Processor requires an upstream connection

## Solutions

### 1. Fix Property Names

The correct property names for ExtractEmailAttachments processor are:

```json
{
  "type": "org.apache.nifi.processors.email.ExtractEmailAttachments",
  "properties": {
    "Attachment Filter": ".*\\.(csv|CSV)$",
    "Extract Attachments Only": "true",
    "Include Original Message": "false",
    "Attachment Name Pattern": ".*",
    "Attachment Size Limit": "100 MB"
  }
}
```

**Key Points:**
- Property names are case-sensitive
- Use exact property names as shown above
- The regex pattern `.*\.(csv|CSV)$` matches CSV files with both lowercase and uppercase extensions

### 2. Ensure Upstream Connection

The processor requires an upstream connection from a GetEmail processor:

```
GetEmail (success) → ExtractEmailAttachments
```

**Connection Steps:**
1. Right-click on GetEmail processor
2. Select "Create Connection"
3. Choose "success" relationship
4. Connect to ExtractEmailAttachments processor

### 3. Alternative Property Names (if above don't work)

If the property names above don't work, try these alternatives:

```json
{
  "properties": {
    "attachment-filter": ".*\\.(csv|CSV)$",
    "extract-attachments-only": "true",
    "include-original-message": "false"
  }
}
```

Or:

```json
{
  "properties": {
    "AttachmentFilter": ".*\\.(csv|CSV)$",
    "ExtractAttachmentsOnly": "true",
    "IncludeOriginalMessage": "false"
  }
}
```

### 4. NiFi Version Compatibility

Check your NiFi version and ensure compatibility:

- **NiFi 1.x**: Use property names with spaces
- **NiFi 2.x+**: May use different property naming conventions

### 5. Manual Configuration Steps

1. **Delete and Recreate the Processor:**
   - Delete the existing ExtractEmailAttachments processor
   - Drag a new ExtractEmailAttachments processor to the canvas
   - Configure properties one by one

2. **Configure Properties Manually:**
   - Right-click on the processor
   - Select "Configure"
   - Go to "Properties" tab
   - Add properties manually:
     - `Attachment Filter`: `.*\.(csv|CSV)$`
     - `Extract Attachments Only`: `true`

3. **Verify Connections:**
   - Ensure GetEmail processor is connected to ExtractEmailAttachments
   - Use "success" relationship from GetEmail

### 6. Validation Checklist

Before starting the processor, verify:

- [ ] GetEmail processor is configured and running
- [ ] Upstream connection exists from GetEmail to ExtractEmailAttachments
- [ ] Property names are exactly as specified
- [ ] Property values are valid (regex pattern, boolean values)
- [ ] No validation errors in processor configuration

### 7. Common Issues and Solutions

**Issue**: "Property not supported" error
**Solution**: Check NiFi version and use appropriate property names

**Issue**: "Invalid regex pattern" error
**Solution**: Test regex pattern separately, ensure proper escaping

**Issue**: "No upstream connection" error
**Solution**: Connect GetEmail processor to ExtractEmailAttachments

**Issue**: Processor won't start
**Solution**: Check all validation errors and fix them before starting

### 8. Testing the Configuration

1. Start GetEmail processor first
2. Verify it's receiving emails
3. Start ExtractEmailAttachments processor
4. Check processor logs for any errors
5. Verify attachments are being extracted

### 9. Log Analysis

Check NiFi logs for detailed error messages:
- Look for property validation errors
- Check for connection issues
- Verify regex pattern compilation

### 10. Fallback Configuration

If all else fails, use this minimal configuration:

```json
{
  "type": "org.apache.nifi.processors.email.ExtractEmailAttachments",
  "properties": {
    "Attachment Filter": ".*\\.csv$",
    "Extract Attachments Only": "true"
  }
}
```

This configuration should work with most NiFi versions and extract CSV attachments from emails.