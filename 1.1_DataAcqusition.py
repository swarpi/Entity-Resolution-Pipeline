# %%
# Define file paths
dblp_file_path = 'dblp.txt'
acm_file_path = 'citation-acm-v8.txt'

def print_first_line(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        first_line = file.readline().strip()
        print(first_line)

# Print the first line of each file
print("First line of DBLP file:")
print_first_line(dblp_file_path)

print("\nFirst line of ACM file:")
print_first_line(acm_file_path)


# %%
def parse_file(file_path):
    papers = []
    paper = {}

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if line.startswith('#*'):  # Title
                # Save the current paper if it exists
                if paper:
                    papers.append(paper)
                    paper = {}
                paper['title'] = line[2:].strip()
            elif line.startswith('#@'):  # Authors
                paper['authors'] = line[2:].strip().split(',')
            elif line.startswith('#t'):  # Year
                paper['year'] = line[2:].strip()
            elif line.startswith('#c'):  # Venue
                paper['venue'] = line[2:].strip()
            elif line.startswith('#index'):  # Paper ID
                paper['id'] = line[6:].strip()
            # Ignore references and abstracts for now
            # elif line.startswith('#%') or line.startswith('#!'):

        # Add the last paper
        if paper:
            papers.append(paper)

    return papers

# Replace with the actual file paths
dblp_papers = parse_file('dblp.txt')
acm_papers = parse_file('citation-acm-v8.txt')

# Example: Print the first paper from each dataset
print("First paper in DBLP dataset:")
print(dblp_papers[0])

print("\nFirst paper in ACM dataset:")
print(acm_papers[0])


# %%
def filter_papers(papers, start_year, end_year, venues):
    filtered_papers = []
    for paper in papers:
        # Extract year and venue, handling cases where they might not be present
        year = int(paper['year']) if 'year' in paper else None
        venue = paper['venue'].lower() if 'venue' in paper else ''

        # Check if the year is within the specified range and if the venue matches the desired venues
        # The 'any()' function checks if any of the venues in 'venues_to_filter' are in the paper's venue
        if year and start_year <= year <= end_year and any(venue_name in venue for venue_name in venues):
            filtered_papers.append(paper)
    return filtered_papers

# Apply filtering
venues_to_filter = ['sigmod', 'vldb']
dblp_papers_filtered = filter_papers(dblp_papers, 1995, 2004, venues_to_filter)
acm_papers_filtered = filter_papers(acm_papers, 1995, 2004, venues_to_filter)

# Example: Print count of filtered papers
print(f"Filtered DBLP papers count: {len(dblp_papers_filtered)}")
print(f"Filtered ACM papers count: {len(acm_papers_filtered)}")


# %%
import csv

def write_to_csv(papers, file_name):
    # Define the header based on the keys of the first paper (assuming all papers have the same keys)
    headers = papers[0].keys() if papers else []

    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)

        # Write the header
        writer.writeheader()

        # Write the paper data
        for paper in papers:
            writer.writerow(paper)

# Write the filtered DBLP and ACM papers to CSV files
write_to_csv(dblp_papers_filtered, 'DBLP 1995-2004.csv')
write_to_csv(acm_papers_filtered, 'ACM 1995-2004.csv')


# %%
def validate_csv(file_name, expected_venues, start_year, end_year):
    with open(file_name, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            # Venue check
            venue = row['venue'].lower()
            if not any(expected_venue.lower() in venue for expected_venue in expected_venues):
                return False, f"Venue check failed for {row['id']}"

            # Date check
            year = int(row['year'])
            if year < start_year or year > end_year:
                return False, f"Date check failed for {row['id']}"

    return True, "All records are valid"

# Example usage
venues_to_check = ['SIGMOD', 'VLDB']
start_year_check = 1995
end_year_check = 2004

# Replace with actual CSV file names
result, message = validate_csv('DBLP 1995-2004.csv', venues_to_check, start_year_check, end_year_check)
print(f"DBLP File Validation: {message}")

result, message = validate_csv('ACM 1995-2004.csv', venues_to_check, start_year_check, end_year_check)
print(f"ACM File Validation: {message}")

# %%



