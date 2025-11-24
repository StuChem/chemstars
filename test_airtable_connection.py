"""
Test Airtable API Connection
This script tests the connection to Airtable and displays sample record structure
"""

import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

AIRTABLE_TOKEN = os.getenv('AIRTABLE_TOKEN')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
# Support both TABLE_NAME and TABLE_ID
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME') or os.getenv('AIRTABLE_TABLE_ID')


def test_connection():
    """Test connection to Airtable and fetch sample records"""
    
    # Validate environment variables
    if not all([AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
        print("❌ Missing environment variables!")
        print("Please create a .env file with:")
        print("  - AIRTABLE_TOKEN")
        print("  - AIRTABLE_BASE_ID")
        print("  - AIRTABLE_TABLE_NAME or AIRTABLE_TABLE_ID")
        return False
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Fetch first 3 records to test
    params = {"maxRecords": 3}
    
    try:
        print(f"🔌 Connecting to Airtable...")
        print(f"   Base ID: {AIRTABLE_BASE_ID}")
        print(f"   Table: {AIRTABLE_TABLE_NAME}\n")
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        records = data.get('records', [])
        
        print(f"✅ Connection successful!")
        print(f"📊 Found {len(records)} sample records\n")
        
        if records:
            print("=" * 60)
            print("SAMPLE RECORD STRUCTURE")
            print("=" * 60)
            print(f"\nRecord ID: {records[0]['id']}")
            print(f"\nAvailable Fields:")
            for i, field_name in enumerate(records[0]['fields'].keys(), 1):
                print(f"  {i}. {field_name}")
            
            print(f"\n" + "=" * 60)
            print("FIRST RECORD DATA")
            print("=" * 60)
            for key, value in records[0]['fields'].items():
                # Truncate long values for display
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                print(f"  {key}: {value}")
            
            print(f"\n" + "=" * 60)
        else:
            print("⚠️ No records found in the table")
        
        return True
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Status Code: {e.response.status_code}")
            print(f"   Response: {e.response.text}")
            
            if e.response.status_code == 401:
                print("\n💡 Tip: Check your AIRTABLE_TOKEN is valid")
            elif e.response.status_code == 404:
                print("\n💡 Tip: Check your AIRTABLE_BASE_ID and AIRTABLE_TABLE_NAME")
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("AIRTABLE CONNECTION TEST")
    print("=" * 60 + "\n")
    
    success = test_connection()
    
    if success:
        print("\n✨ Setup complete! Ready to fetch Airtable data.")
    else:
        print("\n❌ Setup failed. Please check your configuration.")
