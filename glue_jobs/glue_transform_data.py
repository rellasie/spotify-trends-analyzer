import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType, BooleanType
from pyspark.sql.functions import to_timestamp


# initialize glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

input_bucket_name = "raw-data"
processed_bucket_name = "processed-data"
file_names = ["albums.csv", "artists.csv", "features.csv", "tracks.csv"]


# read CSV files from S3
albums_df = spark.read.csv(f"s3://{input_bucket_name}/albums.csv", sep='\t', header=True, inferSchema=True)
features_df = spark.read.csv(f"s3://{input_bucket_name}/features.csv", header=True, sep='\t', inferSchema=True)

# data preprocessing
def clean_albums_df(df):
    return df.withColumn('track_name', F.when(F.col('track_name').isNull() | (F.length(F.trim(F.col('track_name'))) == 0), None)
                          .otherwise(F.trim(F.col('track_name')))) \
             .withColumn('track_id', F.when(F.col('track_id').isNull() | (F.length(F.col('track_id')) != 22), None)
                          .otherwise(F.col('track_id'))) \
             .withColumn('duration_ms', F.when(F.col('duration_ms').isNull() | ~F.col('duration_ms').cast(IntegerType()).isNotNull(), None)
                          .otherwise(F.col('duration_ms').cast(IntegerType()))) \
             .withColumn('album_type', F.when(F.col('album_type').isNull() | (F.length(F.trim(F.col('album_type'))) == 0), 'unknown')
                          .otherwise(F.lower(F.trim(F.col('album_type'))))) \
             .withColumn('release_date', to_timestamp(F.col('release_date'), 'yyyy-MM-dd HH:mm:ss z')) \
             .withColumn('album_popularity', F.when(F.col('album_popularity').isNull() | ~F.col('album_popularity').cast(IntegerType()).isNotNull(), None)
                          .otherwise(F.col('album_popularity').cast(IntegerType()))) \
             .dropDuplicates(['track_id', 'album_id']) \
             .na.drop(subset=['track_id', 'album_id'])

def add_collaboration_metrics(df):
    artist_columns = ['artist_1', 'artist_2', 'artist_3', 'artist_4', 'artist_5', 'artist_6']
    collab_condition = F.lit(False)
    for col in artist_columns:
        collab_condition = collab_condition | F.col(col).isNotNull()
    df = df.withColumn('collab', collab_condition)
    num_collabs_expr = sum([F.when(F.col(col).isNotNull(), 1).otherwise(0) for col in artist_columns])
    return df.withColumn('num_collabs', num_collabs_expr.cast(IntegerType()))

# apply preprocessing

albums_df_clean = clean_albums_df(albums_df)
albums_df_clean = albums_df_clean.withColumn('release_date', F.to_date(F.col('release_date'))) # extract date part in albums_df 
albums_df_clean = add_collaboration_metrics(albums_df_clean)
albums_df_clean = albums_df_clean.dropna(subset=['release_date'])
albums_df_clean = albums_df_clean.withColumn('Year', F.year(F.col('release_date')).cast(IntegerType()))

# process features DataFrame
features_df = features_df.drop('uri', 'track_href', 'analysis_url', 'type')

# repartition
albums_df_clean = albums_df_clean.coalesce(1)
# tracks_df = tracks_df.coalesce(1)
features_df = features_df.coalesce(1)

# write processed data back to S3 in the "process-data" bucket
albums_df_clean.write.csv(f"s3://{processed_bucket_name}/albums", mode="overwrite", sep='\t', header=True)
features_df.write.csv(f"s3://{processed_bucket_name}/features", mode="overwrite", sep='\t', header=True)

job.commit()