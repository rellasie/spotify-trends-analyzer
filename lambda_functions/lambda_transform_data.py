import pandas as pd
import json
import boto3
import io
from io import StringIO
import traceback

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        processing_name = event['key']
        input_bucket = event['bucket']
        output_bucket = "processed-data"

        df = load_data_from_s3(input_bucket, processing_name)

        if 'artist' in processing_name.lower():
            df_processed = clean_artist_df(df)
            output_name = "artist/artist.csv"
        elif 'tracks' in processing_name.lower():
            df_processed = clean_tracks_df(df)
            output_name = "tracks/tracks.csv"
        else:
            raise ValueError(f"Unsupported file type: {processing_name}")


        save_data_to_s3(df_processed, output_bucket, output_name)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'input_file': processing_name,
                'output_file': output_name,
                'output_bucket': output_bucket
            })
        }

    except Exception as e:
        print(f"Error in lambda_handler {str(e)}")
        print("Traceback:")
        print(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }

def load_data_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(content), sep='\t', quotechar = '"')
    print(f"Loaded data columns: {df.columns.tolist()}")
    return df

def save_data_to_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, quotechar = '"', sep='\t')
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    
def drop_nonuseful_columns_artist_df(df):
    null_counts = df.isnull().sum()
    total_rows = len(df)
    columns_to_drop = null_counts[null_counts == total_rows].index.tolist()
    columns_to_drop.extend(['genre_5', 'genre_6'])  # entirely null
    columns_to_drop.extend(['genre_0', 'genre_1', 'genre_2', 'genre_3', 'genre_4'])
    df_cleaned = df.drop(columns=columns_to_drop)
    return df_cleaned

def clean_artist_df(df):
    df = drop_nonuseful_columns_artist_df(df)
    
    valid_genres = ['rock', 'pop', 'jazz', 'classical', 'hip-hop', 'electronic', 'country', 'reggae', 'blues', 'folk']

    def clean_genres(genres):
        if pd.isna(genres) or genres == '':
            return None
        genres_list = genres.replace("'", "").replace("[", "").replace("]", "").split(", ")
        clean_genres = [genre.strip().lower() for genre in genres_list if genre.strip().lower() in valid_genres]
        return ", ".join(clean_genres) if clean_genres else None

    df['id'] = df['id'].where((df['id'].notna()) & (df['id'].str.len() == 22), None)
    df['name'] = df['name'].where((df['name'].notna()) & (df['name'].str.strip().str.len() > 0), None)
    df['name'] = df['name'].str.strip().str.title()
    df['artist_popularity'] = df['artist_popularity'].clip(lower=0, upper=100).fillna(0)
    df['artist_genres'] = df['artist_genres'].apply(clean_genres)
    df['followers'] = pd.to_numeric(df['followers'], errors='coerce').fillna(0).astype(int)
    df = df.drop_duplicates(subset=['id'])

    return df
    
def clean_tracks_df(df):
    df = df.drop_duplicates(subset=['id'])

    return df