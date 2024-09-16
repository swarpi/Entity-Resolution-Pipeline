from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, collect_list, monotonically_increasing_id, first
from pyspark.sql.functions import format_number
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, collect_list, explode, struct
from Levenshtein import distance as lev_distance
import jellyfish

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Entity Resolution Pipeline") \
    .getOrCreate()

# Function to read CSV into DataFrame
def read_csv_to_df(file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

# Read CSV files
dblp_df = read_csv_to_df('DBLP 1995-2004.csv')
acm_df = read_csv_to_df('ACM 1995-2004.csv')


# Print the first few rows to confirm successful file reading
print("First few rows of DBLP DataFrame:")
dblp_df.show(5)

print("First few rows of ACM DataFrame:")
acm_df.show(5)

# Function to perform standard blocking by year
def standard_blocking_by_year(df, block_key):
    return df.groupBy(block_key).agg(collect_list(struct(df.columns)).alias("papers"))

# Apply standard blocking by year
dblp_blocked = standard_blocking_by_year(dblp_df, 'year')
acm_blocked = standard_blocking_by_year(acm_df, 'year')

# UDF for calculating Levenshtein similarity
def levenstein_similarity(str1, str2):
    """Calculate Levenshtein similarity between two strings."""
    max_len = max(len(str1), len(str2))
    if max_len == 0: return 1.0  # Prevent division by zero
    return 1 - lev_distance(str1.lower(), str2.lower()) / max_len

lev_similarity_udf = udf(levenstein_similarity, FloatType())

def find_matches(dblp_df, acm_df, similarity_threshold):
    # Explode the papers lists to compare individual papers
    dblp_exploded = dblp_df.select(explode("papers").alias("dblp_paper"))
    acm_exploded = acm_df.select(explode("papers").alias("acm_paper"))

    # Perform a cross join and filter by similarity threshold
    matches = dblp_exploded.crossJoin(acm_exploded)
    matches = matches.withColumn("similarity_score", 
        lev_similarity_udf(col("dblp_paper.title"), col("acm_paper.title")))
    matches = matches.filter(col("similarity_score") > similarity_threshold)

    matches_selected = matches.select(
        col("dblp_paper.title").alias("DBLP Paper"),
        col("acm_paper.title").alias("ACM Paper"),
        col("similarity_score")
    )
    # Remove duplicate matches based on titles
    unique_matches_no_duplicates = matches_selected.dropDuplicates(["DBLP Paper", "ACM Paper"])
    
    return unique_matches_no_duplicates

# Apply the function
similarity_threshold = 0.8  # adjust this threshold as needed
matched_pairs = find_matches(dblp_blocked, acm_blocked, similarity_threshold)

# Writing matches to a CSV file
matched_pairs.write.csv('Matched_Entities_Para.csv', header=True, mode="overwrite")

print("Matched Pairs:")
matched_pairs.show(5)

