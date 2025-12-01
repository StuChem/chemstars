"""
Airtable ETL Script
Fetches startup data from Airtable, processes connections, and outputs JSON for sigma.js/graphology
"""

import os
import json
import requests
from dotenv import load_dotenv
from collections import defaultdict
from itertools import combinations

# Load environment variables
load_dotenv()

AIRTABLE_TOKEN = os.getenv('AIRTABLE_TOKEN')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_ID = os.getenv('AIRTABLE_TABLE_NAME') or os.getenv('AIRTABLE_TABLE_ID')


def fetch_all_records():
    """Fetch all records from Airtable with pagination"""
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }
    
    all_records = []
    offset = None
    
    print("📥 Fetching records from Airtable...")
    
    while True:
        params = {}
        if offset:
            params['offset'] = offset
            
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            records = data.get('records', [])
            all_records.extend(records)
            
            print(f"   Fetched {len(all_records)} records so far...")
            
            offset = data.get('offset')
            if not offset:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching records: {e}")
            return []
    
    print(f"✅ Total records fetched: {len(all_records)}\n")
    return all_records


def clean_array_field(field_value):
    """Convert Airtable array fields to clean lists"""
    if not field_value:
        return []
    if isinstance(field_value, list):
        return [str(v).strip() for v in field_value if v]
    return [str(field_value).strip()]


def normalize_location(location_str):
    """Normalize location format: 'DE - Berlin' -> 'DE-Berlin'"""
    if not location_str:
        return ''
    return location_str.replace(' - ', '-').replace(' – ', '-').strip()


def process_records(records):
    """Process Airtable records into nodes and edges structure"""
    
    print("🔄 Processing records into nodes and edges...")
    
    nodes = []
    edges_raw = []
    
    # Track which startups share connections
    connection_index = defaultdict(list)  # {connection_value: [startup_ids]}
    
    for record in records:
        record_id = record['id']
        fields = record['fields']
        
        # Extract node data
        startup_name = fields.get('Startup', '').strip()
        if not startup_name:
            continue
        
        # Create node
        node = {
            'id': startup_name,
            'label': startup_name,
            'description': fields.get('Description', ''),
            'website': fields.get('Website', ''),
            'one_liner': fields.get('One liner', ''),
            'location_hq': clean_array_field(fields.get('Location (HQ)', [])),
            'logo_url': fields.get('Logo', [{}])[0].get('url', '') if fields.get('Logo') else ''
        }
        nodes.append(node)
        
        # Extract connection fields
        competencies = clean_array_field(fields.get('Core Competencies', []))
        technical_competencies = clean_array_field(fields.get('Technical Competencies (Test)', []))
        impacts = clean_array_field(fields.get('Impact', []))
        countries = clean_array_field(fields.get('Location (Country)', []))
        cities = clean_array_field(fields.get('Location (City)', []))
        regions = clean_array_field(fields.get('Region', []))
        cohorts = clean_array_field(fields.get('Cohort', []))
        
        # Index connections by type
        for comp in competencies:
            connection_index[('competency', comp)].append(startup_name)
        for tech_comp in technical_competencies:
            connection_index[('technical_competency', tech_comp)].append(startup_name)
        for impact in impacts:
            connection_index[('impact', impact)].append(startup_name)
        for country in countries:
            connection_index[('country', country)].append(startup_name)
        for city in cities:
            connection_index[('city', city)].append(startup_name)
        for region in regions:
            connection_index[('region', region)].append(startup_name)
        for cohort in cohorts:
            connection_index[('cohort', cohort)].append(startup_name)
    
    print(f"   Created {len(nodes)} nodes")
    print(f"   Found {len(connection_index)} unique connection values\n")
    
    # Create edges between startups that share connections
    print("🔗 Creating edges from shared connections...")
    
    combined_edges = defaultdict(lambda: {
        'weight': 0,
        'types': set(),
        'connections': set(),
        'competencies': set(),
        'technical_competencies': set(),
        'impacts': set(),
        'cities': set(),
        'countries': set(),
        'regions': set(),
        'cohorts': set()
    })
    
    for (conn_type, conn_value), startup_list in connection_index.items():
        # Create edges between all pairs of startups sharing this connection
        if len(startup_list) < 2:
            continue
            
        for source, target in combinations(sorted(startup_list), 2):
            # Create consistent edge key (alphabetically ordered)
            edge_key = tuple(sorted([source, target]))
            edge_info = combined_edges[edge_key]
            
            edge_info['weight'] += 1
            edge_info['types'].add(conn_type)
            edge_info['connections'].add(f"{conn_type}: {conn_value}")
            
            # Track by specific type for filtering
            if conn_type == 'competency':
                edge_info['competencies'].add(conn_value)
            elif conn_type == 'technical_competency':
                edge_info['technical_competencies'].add(conn_value)
            elif conn_type == 'impact':
                edge_info['impacts'].add(conn_value)
            elif conn_type == 'city':
                edge_info['cities'].add(conn_value)
            elif conn_type == 'country':
                edge_info['countries'].add(conn_value)
            elif conn_type == 'region':
                edge_info['regions'].add(conn_value)
            elif conn_type == 'cohort':
                edge_info['cohorts'].add(conn_value)
    
    # Build final edges list
    edges = []
    for (source, target), info in combined_edges.items():
        # Create detailed label
        label_parts = []
        if info['competencies']:
            label_parts.append('; '.join(sorted(info['competencies'])))
        if info['technical_competencies']:
            label_parts.append('; '.join(sorted(info['technical_competencies'])))
        if info['impacts']:
            label_parts.append('; '.join(sorted(info['impacts'])))
        if info['cities']:
            label_parts.append('; '.join([normalize_location(c) for c in sorted(info['cities'])]))
        if info['countries']:
            label_parts.append('; '.join([normalize_location(c) for c in sorted(info['countries'])]))
        if info['regions']:
            label_parts.append('; '.join(sorted(info['regions'])))
        if info['cohorts']:
            label_parts.append('; '.join(sorted(info['cohorts'])))
        
        label_detailed = ', '.join(label_parts)
        
        # Truncate if too long
        MAX_LABEL = 120
        if len(label_detailed) > MAX_LABEL:
            label_detailed = label_detailed[:MAX_LABEL-1].rstrip(', ') + '…'
        
        edge = {
            'source': source,
            'target': target,
            'weight': info['weight'],
            'label': f"{info['weight']} connections",
            'label_detailed': label_detailed,
            'types': list(info['types']),
            'is_competency': 'competency' in info['types'],
            'is_technical_competency': 'technical_competency' in info['types'],
            'is_impact': 'impact' in info['types'],
            'is_city': 'city' in info['types'],
            'is_country': 'country' in info['types'],
            'is_region': 'region' in info['types'],
            'is_cohort': 'cohort' in info['types'],
            'competencies': list(info['competencies']),
            'technical_competencies': list(info['technical_competencies']),
            'impacts': list(info['impacts']),
            'cities': list(info['cities']),
            'countries': list(info['countries']),
            'regions': list(info['regions']),
            'cohorts': list(info['cohorts'])
        }
        edges.append(edge)
    
    print(f"   Created {len(edges)} edges\n")
    
    return nodes, edges


def export_json(nodes, edges, output_file='network_data.json'):
    """Export nodes and edges to JSON format for sigma.js/graphology"""
    
    print(f"💾 Exporting to {output_file}...")
    
    data = {
        'nodes': nodes,
        'edges': edges,
        'metadata': {
            'total_nodes': len(nodes),
            'total_edges': len(edges),
            'generated_at': None  # Will be set by JavaScript Date
        }
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Export complete!\n")
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Nodes: {len(nodes)}")
    print(f"Edges: {len(edges)}")
    print(f"Output file: {output_file}")
    print("=" * 60)


def main():
    """Main ETL process"""
    
    print("=" * 60)
    print("AIRTABLE ETL - NETWORK DATA PROCESSOR")
    print("=" * 60 + "\n")
    
    # Validate environment variables
    if not all([AIRTABLE_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID]):
        print("❌ Missing environment variables!")
        print("Please ensure .env file contains:")
        print("  - AIRTABLE_TOKEN")
        print("  - AIRTABLE_BASE_ID")
        print("  - AIRTABLE_TABLE_ID (or AIRTABLE_TABLE_NAME)")
        return
    
    # Fetch records
    records = fetch_all_records()
    if not records:
        print("❌ No records fetched. Exiting.")
        return
    
    # Process into nodes and edges
    nodes, edges = process_records(records)
    
    # Export JSON
    export_json(nodes, edges)
    
    print("\n✨ ETL process complete!")


if __name__ == "__main__":
    main()
