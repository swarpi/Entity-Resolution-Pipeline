import csv
from collections import defaultdict
from itertools import product
import time
import Levenshtein as lev
import jellyfish
import random

def read_csv(file_path):
    #Read a CSV file and return a list of dictionaries representing rows
    with open(file_path, mode='r', encoding='utf-8') as file:
        return list(csv.DictReader(file))

def sorted_neighborhood_method(papers, key, window_size):
    #Block papers using a Sorted Neighborhood Method
    sorted_papers = sorted(papers, key=lambda x: x.get(key, ''))
    blocks = defaultdict(list)
    for i in range(len(sorted_papers) - window_size + 1):
        for j in range(i, i + window_size):
            block_key = f"Block_{i // (window_size // 2)}"
            blocks[block_key].append(sorted_papers[j])
    return blocks

def standard_blocking_by_year(papers, block_key):
    #Group papers into blocks based on the block_key.
    #Returns a dictionary where each key is a block value, and the value is a list of papers in that block.
    blocks = defaultdict(list)
    for paper in papers:
        key = paper[block_key]
        blocks[key].append(paper)
    return blocks

def combine_blocks(dblp_blocks, acm_blocks):
    #Combine DBLP and ACM blocks into a single structure for comparison
    combined_blocks = {}
    for key in set(dblp_blocks.keys()).union(set(acm_blocks.keys())):
        combined_blocks[key] = {
            'DBLP': dblp_blocks.get(key, []),
            'ACM': acm_blocks.get(key, [])
        }
    return combined_blocks

def generate_ngrams(text, n):
    #Generate n-grams from a given text
    return [text[i:i+n] for i in range(len(text)-n+1)]

def ngram_blocking(papers, block_key, n):
    #Block papers based on n-grams of a specified field.
    blocks = defaultdict(list)
    for paper in papers:
        if block_key in paper and paper[block_key]:
            ngrams = generate_ngrams(paper[block_key].lower(), n)
            for ngram in ngrams:
                blocks[ngram].append(paper)
    return blocks

def levenstein_similarity(str1, str2):
    #Calculate Levenshtein similarity between two strings.
    return 1 - lev.distance(str1.lower(), str2.lower()) / max(len(str1), len(str2))

def phonetic_similarity(str1, str2):
    #Calculate Phonetic similarity between two strings.
    return jellyfish.soundex(str1) == jellyfish.soundex(str2)

def jaccard_similarity(str1, str2):
    #Calculate Jaccard similarity between two sets.
    set1 = set(str1.lower())
    set2 = set(str2.lower())
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union

def compare_entities(block, similarity_func, similarity_threshold):
    matches = set()
    for dblp_paper, acm_paper in product(block['DBLP'], block['ACM']):
        title_pair = (dblp_paper['title'], acm_paper['title'])
        if title_pair not in matches:  # Checking for duplicates
            similarity_score = similarity_func(dblp_paper['title'], acm_paper['title'])
            if similarity_score > similarity_threshold:
                matches.add(title_pair + (similarity_score,))
    return list(matches)



def calculate_precision_recall_fmeasure(identified_matches, baseline_matches, threshold):
    # Convert matches to a comparable format (set of tuples)
    identified_set = {(m[0], m[1]): m[2] for m in identified_matches}
    baseline_set = {(m[0], m[1]) for m in baseline_matches}

    # True Positives: Identified matches that are in the baseline
    true_positives = sum(1 for pair, score in identified_set.items() if pair in baseline_set and score >= threshold)
    
    # False Positives: Identified matches not in the baseline or below the threshold
    false_positives = sum(1 for pair, score in identified_set.items() if pair not in baseline_set or score < threshold)
    
    # False Negatives: Baseline matches not identified
    false_negatives = sum(1 for pair in baseline_set if pair not in identified_set or identified_set[pair] < threshold)

    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) else 0
    f_measure = 2 * (precision * recall) / (precision + recall) if (precision + recall) else 0

    return precision, recall, f_measure


# Read CSV files
dblp_file_path = 'DBLP 1995-2004.csv'
acm_file_path = 'ACM 1995-2004.csv'
dblp_papers = read_csv(dblp_file_path)
acm_papers = read_csv(acm_file_path)

# Implement Sorted Neighborhood Method for blocking
window_size = 5  # Adjust as needed
dblp_blocks_snm = sorted_neighborhood_method(dblp_papers, 'title', window_size)
acm_blocks_snm = sorted_neighborhood_method(acm_papers, 'title', window_size)

# Perform standard blocking on both datasets using 'year' as the block key
dblp_blocks = standard_blocking_by_year(dblp_papers, 'year')
acm_blocks = standard_blocking_by_year(acm_papers, 'year')

# Combine blocks from both datasets
combined_blocks_snm = combine_blocks(dblp_blocks_snm, acm_blocks_snm)
combined_blocks_year = combine_blocks(dblp_blocks,acm_blocks)

ngram_size = 5  # For trigrams
dblp_blocks_ngram = ngram_blocking(dblp_papers, 'title', ngram_size)
acm_blocks_ngram = ngram_blocking(acm_papers, 'title', ngram_size)

combined_blocks_ngram = combine_blocks(dblp_blocks_ngram, acm_blocks_ngram)

def calculate_and_print_metrics(block_type, blocks, baseline_matches, similarity_threshold, similarity_func):
    start_time = time.time()
    all_matches = set()
    for block in blocks.values():
        block_matches = compare_entities(block, similarity_func, similarity_threshold)
        all_matches.update(block_matches)
    precision, recall, f_measure = calculate_precision_recall_fmeasure(all_matches, baseline_matches, similarity_threshold)
    duration = time.time() - start_time
    print(f"{block_type} ({similarity_func.__name__}) execution time: {duration:.2f} seconds")
    print(f"Precision ({block_type}): {precision:.4f}")
    print(f"Recall ({block_type}): {recall:.4f}")
    print(f"F-measure ({block_type}): {f_measure:.4f}")
    return list(all_matches)

def calculate_and_print_baseline_metrics(db_papers, acm_papers, similarity_func, similarity_threshold):
    #Simulate baseline generation for comparison.
    start_time = time.time()
    simulated_baseline_matches = set()  # Ensure this is a list
    
    # Loop through all paper combinations and calculate similarity
    for dblp_paper in db_papers:
        for acm_paper in acm_papers:
            similarity_score = similarity_func(dblp_paper['title'], acm_paper['title'])
            if similarity_score >= similarity_threshold:
                # Append matches that meet or exceed the similarity threshold
                simulated_baseline_matches.add((dblp_paper['title'], acm_paper['title'], similarity_score))
    
    duration = time.time() - start_time
    print(f"Baseline simulation execution time: {duration:.2f} seconds")
    print(f"Simulated Baseline Matches: {len(simulated_baseline_matches)}")

    # Since this part of the code is intended for simulation and illustration, 
    # actual baseline comparison logic may vary depending on your application's needs.
    
    return simulated_baseline_matches





# Read CSV files and perform initial data processing...
# Code for reading CSVs, generating blocks, etc., remains unchanged...
similarity_threshold = 0.8
# Baseline Timing and Calculation
start_baseline_time = time.time()
baseline_matches = calculate_and_print_baseline_metrics(dblp_papers, acm_papers, levenstein_similarity, similarity_threshold)
end_baseline_time = time.time()
baseline_duration = end_baseline_time - start_baseline_time
print(f"Baseline generation time: {baseline_duration:.2f} seconds")

# Blocking Schema Calculation and Printing Metrics
# Calculate metrics for each blocking method and similarity function
# Phonetic Similarity
all_matches_snm_phonetic = calculate_and_print_metrics("SNM", combined_blocks_snm, baseline_matches, similarity_threshold, phonetic_similarity)
all_matches_ngram_phonetic = calculate_and_print_metrics("N-gram", combined_blocks_ngram, baseline_matches, similarity_threshold, phonetic_similarity)
all_matches_year_phonetic = calculate_and_print_metrics("Year", combined_blocks_year, baseline_matches, similarity_threshold, phonetic_similarity)

# Levenshtein Similarity
all_matches_snm_levenstein = calculate_and_print_metrics("SNM", combined_blocks_snm, baseline_matches, similarity_threshold, levenstein_similarity)
all_matches_ngram_levenstein = calculate_and_print_metrics("N-gram", combined_blocks_ngram, baseline_matches, similarity_threshold, levenstein_similarity)
all_matches_year_levenstein = calculate_and_print_metrics("Year", combined_blocks_year, baseline_matches, similarity_threshold, levenstein_similarity)

# Jaccard Similarity#
all_matches_snm_jaccard = calculate_and_print_metrics("SNM", combined_blocks_snm, baseline_matches, similarity_threshold, jaccard_similarity)
all_matches_ngram_jaccard = calculate_and_print_metrics("N-gram", combined_blocks_ngram, baseline_matches, similarity_threshold, jaccard_similarity)
all_matches_year_jaccard = calculate_and_print_metrics("Year", combined_blocks_year, baseline_matches, similarity_threshold, jaccard_similarity)

def write_matches_to_csv(matches, filename="Matched Entities.csv"):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['DBLP Paper', 'ACM Paper', 'Similarity Score'])
        for match in matches:
            writer.writerow(match)

# Write baseline matches to CSV
write_matches_to_csv(all_matches_year_levenstein)
