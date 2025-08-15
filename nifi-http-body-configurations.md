# NiFi InvokeHTTP Body Configurations for New Relic API

## 1. Standard REST API Endpoints (No Body Required)

For most New Relic REST API endpoints, you use GET requests without a body:

```yaml
Processor: InvokeHTTP
Properties:
  - HTTP Method: GET
  - Remote URL: https://api.newrelic.com/v2/applications.json
  - Send Message Body: false
  
Headers:
  - X-Api-Key: ${newrelic.api.key}
  - Accept: application/json
```

## 2. New Relic GraphQL API (NerdGraph) - POST with Body

For complex queries using NerdGraph, you need a POST request with a GraphQL query body:

### InvokeHTTP Configuration:
```yaml
Processor: InvokeHTTP
Properties:
  - HTTP Method: POST
  - Remote URL: https://api.newrelic.com/graphql
  - Send Message Body: true
  - Content-Type: application/json
  
Headers:
  - API-Key: ${newrelic.api.key}
  - Content-Type: application/json
```

### Request Body Examples:

#### A. Query Application Performance Data:
```json
{
  "query": "{ actor { account(id: YOUR_ACCOUNT_ID) { nrql(query: \"SELECT average(duration) FROM Transaction WHERE appName = 'Your App Name' SINCE 1 hour ago TIMESERIES\") { results } } } }"
}
```

#### B. Query Infrastructure Data:
```json
{
  "query": "{ actor { account(id: YOUR_ACCOUNT_ID) { nrql(query: \"SELECT average(cpuPercent), average(memoryUsedPercent) FROM SystemSample WHERE hostname LIKE '%prod%' SINCE 30 minutes ago FACET hostname\") { results } } } }"
}
```

#### C. Query Custom Events:
```json
{
  "query": "{ actor { account(id: YOUR_ACCOUNT_ID) { nrql(query: \"SELECT count(*) FROM PageView WHERE userAgentName = 'Chrome' SINCE 1 day ago FACET countryCode\") { results } } } }"
}
```

## 3. NRQL Insights API - GET with Query Parameters

For NRQL queries using the Insights API, you can use GET with parameters or POST with body:

### Option A: GET Request (No Body)
```yaml
Processor: InvokeHTTP
Properties:
  - HTTP Method: GET
  - Remote URL: https://insights-api.newrelic.com/v1/accounts/${newrelic.account.id}/query?nrql=SELECT%20*%20FROM%20Transaction%20SINCE%201%20hour%20ago
  
Headers:
  - X-Query-Key: ${newrelic.query.key}
```

### Option B: POST Request (With Body)
```yaml
Processor: InvokeHTTP
Properties:
  - HTTP Method: POST
  - Remote URL: https://insights-api.newrelic.com/v1/accounts/${newrelic.account.id}/query
  - Send Message Body: true
  - Content-Type: application/json
  
Headers:
  - X-Query-Key: ${newrelic.query.key}
  - Content-Type: application/json
```

#### Request Body for NRQL:
```json
{
  "nrql": "SELECT average(duration), count(*) FROM Transaction WHERE appName = 'My Application' SINCE 1 hour ago TIMESERIES AUTO"
}
```

## 4. Dynamic Body Generation in NiFi

### Using ReplaceText Processor to Generate Body

```yaml
Processor: ReplaceText (before InvokeHTTP)
Properties:
  - Search Value: .*
  - Replacement Value: |
    {
      "query": "{ 
        actor { 
          account(id: ${newrelic.account.id}) { 
            nrql(query: \"SELECT average(duration) FROM Transaction WHERE appName = '${app.name}' SINCE ${time.window} TIMESERIES\") { 
              results 
            } 
          } 
        } 
      }"
    }
  - Replacement Strategy: Always Replace
```

### Using GenerateFlowFile to Create Request Body

```yaml
Processor: GenerateFlowFile
Properties:
  - File Size: 0B
  - Custom Text: |
    {
      "query": "{ actor { account(id: ${newrelic.account.id}) { nrql(query: \"SELECT * FROM Transaction SINCE 1 hour ago LIMIT 100\") { results } } } }"
    }
```

## 5. Complex Multi-Query Body

For fetching multiple metrics in one request:

```json
{
  "query": "query($accountId: Int!) {
    actor {
      account(id: $accountId) {
        applications: nrql(query: \"SELECT uniqueCount(appName) FROM Transaction SINCE 1 day ago\") {
          results
        }
        responseTime: nrql(query: \"SELECT average(duration) FROM Transaction SINCE 1 hour ago TIMESERIES\") {
          results
        }
        errorRate: nrql(query: \"SELECT percentage(count(*), WHERE error IS true) FROM Transaction SINCE 1 hour ago TIMESERIES\") {
          results
        }
        throughput: nrql(query: \"SELECT rate(count(*), 1 minute) FROM Transaction SINCE 1 hour ago TIMESERIES\") {
          results
        }
      }
    }
  }",
  "variables": {
    "accountId": YOUR_ACCOUNT_ID
  }
}
```

## 6. Template-Based Body with Variables

### Using EvaluateJsonPath to Extract Parameters

```yaml
# First processor: EvaluateJsonPath
Processor: EvaluateJsonPath
Properties:
  - app.name: $.application_name
  - time.range: $.time_range
  - metric.type: $.metric_type

# Second processor: ReplaceText  
Processor: ReplaceText
Properties:
  - Replacement Value: |
    {
      "query": "{ 
        actor { 
          account(id: ${newrelic.account.id}) { 
            nrql(query: \"SELECT ${metric.type} FROM Transaction WHERE appName = '${app.name}' SINCE ${time.range}\") { 
              results 
            } 
          } 
        } 
      }"
    }
```

## 7. Batch Queries for Multiple Applications

```json
{
  "query": "query GetMultipleApps($apps: [String!]!) {
    actor {
      account(id: YOUR_ACCOUNT_ID) {
        app1: nrql(query: \"SELECT average(duration) FROM Transaction WHERE appName = $apps[0] SINCE 1 hour ago\") {
          results
        }
        app2: nrql(query: \"SELECT average(duration) FROM Transaction WHERE appName = $apps[1] SINCE 1 hour ago\") {
          results  
        }
        app3: nrql(query: \"SELECT average(duration) FROM Transaction WHERE appName = $apps[2] SINCE 1 hour ago\") {
          results
        }
      }
    }
  }",
  "variables": {
    "apps": ["App1", "App2", "App3"]
  }
}
```

## 8. Error Handling for Body Requests

### Configure InvokeHTTP for Error Responses

```yaml
Processor: InvokeHTTP
Properties:
  - HTTP Method: POST
  - Put Response Body In Attribute: response.body
  - Always Output Response: true
  - Penalize No Retry: false
  
Relationships:
  - Original: (route to cleanup)
  - Response: (route to ProcessResponseBody)
  - No Retry: (route to LogError)
  - Failure: (route to LogError)
```

## 9. Authentication with Body Requests

### For GraphQL/NerdGraph:
```yaml
Headers:
  - API-Key: ${newrelic.api.key}
  - Content-Type: application/json
  - User-Agent: NiFi-NewRelic-Connector/1.0
```

### For Insights API:
```yaml
Headers:
  - X-Query-Key: ${newrelic.query.key}
  - Content-Type: application/json
```

## 10. Complete NiFi Flow Example with Body

```
GenerateFlowFile (trigger) 
  ↓
ReplaceText (create GraphQL body)
  ↓  
InvokeHTTP (POST to NerdGraph)
  ↓
EvaluateJsonPath (extract response data)
  ↓
SplitJson (split results array)
  ↓
ConvertRecord (JSON to CSV)
  ↓
PutSnowflakeInternalStage
```

## Environment Variables for Body Templates

Set these in NiFi variables:

```bash
# Account and Authentication
newrelic.account.id=1234567
newrelic.api.key=NRAK-XXXXX
newrelic.query.key=NRIQ-XXXXX

# Query Parameters  
app.name=MyApplication
time.window=1 hour ago
metric.names=duration,throughput,errorRate

# GraphQL Endpoint
newrelic.graphql.url=https://api.newrelic.com/graphql
```