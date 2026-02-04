"""
Diagnostic script to check why account is still blocked after overnight wait
"""
import requests
import json
import time

API_KEY = "Zo7hh3b9aYF7Fpsba_hb"  # Your key from debugger

print("="*60)
print("NASDAQ ACCOUNT DIAGNOSTIC TEST")
print("="*60)
print(f"Test time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"API Key: {API_KEY[:10]}...")
print()

# Test 1: Simple TICKERS request
url = "https://data.nasdaq.com/api/v3/datatables/SHARADAR/TICKERS.json"
params = {"api_key": API_KEY}

print("🔍 Test 1: Making single API request to SHARADAR/TICKERS...")
response = requests.get(url, params=params, timeout=30)

print(f"   Status Code: {response.status_code}")
print()

# Inspect response headers
print("📋 Response Headers:")
for key, value in response.headers.items():
    if 'rate' in key.lower() or 'retry' in key.lower() or 'limit' in key.lower():
        print(f"   {key}: {value}")
print()

# Inspect response body
print("📄 Response Body:")
try:
    response_json = response.json()
    print(json.dumps(response_json, indent=2))
except:
    print(f"   (Raw text) {response.text[:500]}")
print()

# Verdict
print("="*60)
print("VERDICT:")
print("="*60)
if response.status_code == 200:
    print("✅ Account is ACTIVE and UNBLOCKED")
    print("   Safe to run your downloader.")
elif response.status_code == 429:
    print("❌ Account is still RATE LIMITED")
    print()
    print("Possible reasons:")
    print("1. Account suspension (not just rate limit)")
    print("2. IP address block")
    print("3. Invalid or expired API key")
    print("4. Premium subscription expired")
    print()
    print("ACTION REQUIRED:")
    print("Contact Nasdaq Data Link support:")
    print("  Email: clientsuccess@nasdaq.com")
    print(f"  Subject: 'Account {API_KEY[:10]}... still rate limited after 24+ hours'")
    print("  Include: Screenshot of this output")
elif response.status_code == 401 or response.status_code == 403:
    print("❌ AUTHENTICATION ERROR")
    print("   Your API key may be invalid, expired, or suspended.")
    print("   Check your account at: https://data.nasdaq.com/account")
else:
    print(f"⚠️  Unexpected status: {response.status_code}")
    print("   Contact support with this diagnostic output.")
