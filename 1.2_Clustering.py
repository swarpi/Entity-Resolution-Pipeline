import csv
from collections import defaultdict
import random
from CompareDataset import DatasetComparer

def read_csv(file_path):
    """Read a CSV file and return a list of dictionaries representing rows."""
    with open(file_path, mode='r', encoding='utf-8') as file:
        return list(csv.DictReader(file))

def read_matched_entities(file_path):
    """Read the Matched Entities CSV file and return a list of matched pairs."""
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return [(row['DBLP Paper'], row['ACM Paper'], float(row['Similarity Score'])) for row in reader]
    
    
# Write the updated datasets to new CSV files
def write_to_csv(filename, records):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        for record in records:
            writer.writerow(record)

# Read matched entities from the CSV file
matched_entities_file = 'Matched Entities.csv'
matched_entities = read_matched_entities(matched_entities_file)

# Read CSV files
dblp_file_path = 'DBLP 1995-2004.csv'
acm_file_path = 'ACM 1995-2004.csv'
dblp_papers = read_csv(dblp_file_path)
acm_papers = read_csv(acm_file_path)


def map_titles_to_records(papers):
    """Map titles to their corresponding full records."""
    return {paper['title']: paper for paper in papers}

# Map titles to records for both datasets
dblp_dict = map_titles_to_records(dblp_papers)
acm_dict = map_titles_to_records(acm_papers)

def form_clusters(matched_pairs):
    clusters = defaultdict(set)
    for dblp_title, acm_title, _ in matched_pairs:
        cluster_key = dblp_title if dblp_title in dblp_dict else acm_title
        clusters[cluster_key].add(dblp_title)
        clusters[cluster_key].add(acm_title)
    return clusters

def resolve_clusters_to_records(clusters, dblp_dict, acm_dict):
    resolved_records = {}
    for cluster_key, titles in clusters.items():
        # Choose the representative title (e.g., first title in the set)
        representative_title = next(iter(titles))
        resolved_record = dblp_dict.get(representative_title, acm_dict.get(representative_title))
        for title in titles:
            resolved_records[title] = resolved_record
    return resolved_records

clusters = form_clusters(matched_entities)
resolved_records = resolve_clusters_to_records(clusters, dblp_dict, acm_dict)

# Update the original datasets with resolved records
def update_dataset_with_resolved_records(dataset, resolved_records):
    updated_dataset = []
    for record in dataset:
        title = record['title']
        if title in resolved_records:
            updated_record = {**record, **resolved_records[title]}
            updated_dataset.append(updated_record)
        else:
            updated_dataset.append(record)
    return updated_dataset

updated_dblp_papers = update_dataset_with_resolved_records(dblp_papers, resolved_records)
updated_acm_papers = update_dataset_with_resolved_records(acm_papers, resolved_records)

# Write to CSV
write_to_csv('Updated_DBLP.csv', updated_dblp_papers)
write_to_csv('Updated_ACM.csv', updated_acm_papers)

# Assuming 'updated_dblp_papers' and 'updated_acm_papers' are your updated datasets

# Step 1: Initialize the DatasetComparer class
comparer = DatasetComparer(updated_dblp_papers, updated_acm_papers)

# Step 2: Use class methods
# To compare a specific paper
specific_title = "A Sample Paper Title"  # Replace with an actual title from your dataset
similarity_score = comparer.compare_papers(specific_title)
if similarity_score is not None:
    print(f"Similarity for '{specific_title}': {similarity_score:.2f}")
else:
    print(f"Title '{specific_title}' not found in both datasets.")

# To compare all papers present in both datasets
all_similarities = comparer.compare_all_papers()
for title, sim in all_similarities.items():
    print(f"Title: {title}, Similarity: {sim:.2f}")

