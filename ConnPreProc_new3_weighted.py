import pandas as pd
import itertools
from collections import defaultdict
import re

# Read the full connections dataset
edges_df = pd.read_csv('startup_connections_full.csv', encoding='utf-8')
nodes_df = pd.read_csv('startup_nodes_full.csv', encoding='utf-8')

# Create a dictionary to store combined edges
combined_edges = defaultdict(lambda: {'weight': 0, 'types': set(), 'connections': set()})

# Combine edges between the same nodes
for _, row in edges_df.iterrows():
    # Create a consistent key for each pair of nodes (alphabetically ordered)
    node_pair = tuple(sorted([row['Source'], row['Target']]))
    edge_info = combined_edges[node_pair]
    edge_info['weight'] += 1
    edge_info['types'].add(row['Type'])
    edge_info['connections'].add(row['Connection_full'])

# Convert combined edges to DataFrame
weighted_edges = []
for (source, target), info in combined_edges.items():
    # Build a parsed mapping from connection strings like 'city: DE - Berlin'
    parsed = {'city': [], 'country': [], 'competency': [], 'impact': [], 'target_market': [], 'other': []}
    for conn in info['connections']:
        # Expect format 'type: value' (this matches how Connection_full was created)
        if isinstance(conn, str) and ': ' in conn:
            t, v = conn.split(': ', 1)
            t = t.strip().lower()
            v = v.strip()
            if t in parsed:
                parsed[t].append(v)
            else:
                parsed['other'].append(v)
        else:
            parsed['other'].append(str(conn))

    # Pick the 'connection name' as the first non-location value if available
    connection_name = ''
    for cand_type in ('competency', 'impact', 'target_market'):
        if parsed[cand_type]:
            connection_name = parsed[cand_type][0]
            break
    if not connection_name and parsed['other']:
        connection_name = parsed['other'][0]

    # Format city and country (normalize 'DE - Berlin' -> 'DE-Berlin' as example)
    city = parsed['city'][0] if parsed['city'] else ''
    country = parsed['country'][0] if parsed['country'] else ''
    competency = parsed['competency'][0] if parsed['competency'] else ''

    def norm_location(s):
        return s.replace(' - ', '-').replace(' – ', '-').strip()

    city = norm_location(city) if city else ''
    country = norm_location(country) if country else ''

    # Build the detailed label from explicit fields (no connection_name fallback):
    # Order: Target, Competency(s), Impact(s), TargetMarket(s), City, Country
    # Prepare lists for multi-valued fields (deduped, preserve order)
    def dedupe_preserve_order(seq):
        seen = set()
        out = []
        for s in seq:
            key = ' '.join(s.split()).lower()
            if key and key not in seen:
                seen.add(key)
                out.append(s)
        return out

    # Extract lists from parsed mapping
    competencies = dedupe_preserve_order(parsed['competency'])
    impacts = dedupe_preserve_order(parsed['impact'])
    markets = dedupe_preserve_order(parsed['target_market'])

    # Join multi-valued fields with semicolon
    comp_str = ';'.join(competencies) if competencies else ''
    imp_str = ';'.join(impacts) if impacts else ''
    market_str = ';'.join(markets) if markets else ''

    # City and country are single-valued (first encountered)
    city_str = city
    country_str = country

    # Put Source then Target first so both node names appear in the edge label
    raw_parts = []
    # include source and target as the first tokens (source first)
    if source:
        raw_parts.append(source)
    if target and target != source:
        raw_parts.append(target)
    # then include connection attributes: Competency(s), Impact(s), TargetMarket(s), City, Country
    if comp_str:
        raw_parts.append(comp_str)
    if imp_str:
        raw_parts.append(imp_str)
    if market_str:
        raw_parts.append(market_str)
    if city_str:
        raw_parts.append(city_str)
    if country_str:
        raw_parts.append(country_str)

    # Deduplicate tokens while preserving order (case-insensitive). For semicolon-joined tokens we treat
    # the whole chunk as a single token but we already deduped entries inside those chunks.
    seen_tokens = set()
    final_parts = []
    for token in raw_parts:
        key = ' '.join(token.split()).lower()
        if key and key not in seen_tokens:
            seen_tokens.add(key)
            final_parts.append(token)

    label_detailed = ', '.join(final_parts)

    # Truncate label if too long
    LABEL_MAX = 120
    if len(label_detailed) > LABEL_MAX:
        label_detailed = label_detailed[:LABEL_MAX-1].rstrip(', ').rstrip() + '…'

    weighted_edges.append({
        'Source': source,
        'Target': target,
        'Weight': info['weight'],
        'Types': ';'.join(sorted(info['types'])),
        'Connections': ';'.join(sorted(info['connections'])),
        'Label': f"{info['weight']} connections: " + ", ".join(sorted(info['types'])),  # summary label
        'LabelDetailed': label_detailed,  # explicit detailed label for Gephi
        'is_competency': 1 if 'competency' in info['types'] else 0,
        'is_impact': 1 if 'impact' in info['types'] else 0,
        'is_city': 1 if 'city' in info['types'] else 0,
        'is_country': 1 if 'country' in info['types'] else 0,
        'is_target_market': 1 if 'target_market' in info['types'] else 0
    })

weighted_edges_df = pd.DataFrame(weighted_edges)

# Export weighted dataset
weighted_edges_df.to_csv('startup_connections_weighted.csv', index=False, encoding='utf-8')

print("\nWeighted dataset statistics:")
print(f"Nodes: {len(nodes_df)}")
print(f"Original edges: {len(edges_df)}")
print(f"Weighted edges: {len(weighted_edges_df)}")

print("\nWeight distribution:")
print(weighted_edges_df['Weight'].describe())

# Print sample of weighted edges
print("\nSample of weighted edges (showing high-weight connections):")
sample = weighted_edges_df.nlargest(10, 'Weight')[['Source', 'Target', 'Weight', 'Types', 'LabelDetailed']]
print(sample.to_string(index=False))