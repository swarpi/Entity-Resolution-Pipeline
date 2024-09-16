from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_list, monotonically_increasing_id, struct, col, udf, first, coalesce
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import Levenshtein as lev

# Initialize Spark Session
spark = SparkSession.builder.appName("Entity Resolution Pipeline").getOrCreate()

# Read matched pairs into a DataFrame
matched_pairs_schema = StructType([
    StructField("DBLP Paper", StringType(), True),
    StructField("ACM Paper", StringType(), True),
    StructField("Similarity Score", FloatType(), True)
])
# Function to read CSV into DataFrame
def read_csv_to_df(file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

# Read CSV files
dblp_df = read_csv_to_df('DBLP 1995-2004.csv')
acm_df = read_csv_to_df('ACM 1995-2004.csv')
matched_pairs_df = spark.read.csv('Matched Entities.csv', header=True, schema=matched_pairs_schema)

# Assign a unique ID to each row (cluster ID)
matched_pairs_df = matched_pairs_df.withColumn("cluster_id", monotonically_increasing_id())

# Form clusters
clusters_df = matched_pairs_df.groupBy("cluster_id").agg(
    collect_list("DBLP Paper").alias("dblp_titles"),
    collect_list("ACM Paper").alias("acm_titles")
)

# Resolve clusters to records (choosing representative title)
resolve_udf = udf(lambda titles: titles[0], StringType())
resolved_clusters_df = clusters_df.withColumn("representative_title", resolve_udf(clusters_df["dblp_titles"]))

resolved_clusters_df.show(5)

# Write updated datasets to CSV

# Create mapping from titles to representative titles using the resolved_clusters_df
title_mapping_df = resolved_clusters_df.select("dblp_titles", "representative_title", "acm_titles")\
                                       .distinct()
                                       
print("title mapping")
title_mapping_df.show(5)
print("dplp df")
dblp_df.show(5)
# Join and update DBLP DataFrame
# Explode the dblp_titles array to create a proper mapping
exploded_title_mapping_df_1 = resolved_clusters_df.select(explode("dblp_titles").alias("dblp_title"), "representative_title", "acm_titles").distinct()
# Then, explode "acm_titles" on the result
exploded_title_mapping_df = exploded_title_mapping_df_1.select("dblp_title", "representative_title", explode("acm_titles").alias("acm_title")).distinct()

print("title mapping exploding")
exploded_title_mapping_df.show(20)
# Alias for clear column referencing
dblp_df_alias = dblp_df.alias("dblp")
exploded_title_mapping_df_alias = exploded_title_mapping_df.alias("mapping")
print("alias")
exploded_title_mapping_df_alias.show(20)
print("alias dblp")
dblp_df_alias.show(5)
# Join and update DBLP DataFrame
updated_dblp_df = dblp_df_alias.join(exploded_title_mapping_df_alias, col("dblp.title") == col("mapping.dblp_title"), "left_outer")\
                               .select(col("mapping.representative_title"), *([col("dblp." + xx) for xx in dblp_df.columns if xx != "title"]))

                         
updated_dblp_df.show(5)
# Join and update ACM DataFrame
acm_df_alias = acm_df.alias("acm")

# Join and update ACM DataFrame
updated_acm_df = acm_df_alias.join(exploded_title_mapping_df_alias, col("acm.title") == col("mapping.acm_title"), "left_outer")\
                               .select(col("mapping.representative_title"), *([col("acm." + xx) for xx in acm_df.columns if xx != "title"]))
        
updated_acm_df.show(5)
# Write updated datasets to CSV
exploded_title_mapping_df_alias.write.csv('Updated_title_para.csv', header=True, mode="overwrite")
updated_dblp_df.write.csv('Updated_DBLP_para.csv', header=True, mode="overwrite")
updated_acm_df.write.csv('Updated_ACM_para.csv', header=True, mode="overwrite")