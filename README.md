# Spotify Trends Analyzer

## Project Overview

Spotify Trends Analyzer is a comprehensive data analysis project that utilizes AWS services to process and visualize Spotify data, providing valuable insights for the music industry. This project aims to help artists, producers, and music management companies make data-driven decisions in music creation, production, and release strategies.

## Features

- Large-scale processing of Spotify dataset (artists, albums, tracks, audio features)
- Trend analysis of music characteristics over time
- Release day popularity analysis
- Track duration vs. popularity analysis
- Interactive visualizations and dashboards

## Tech Stack

- Amazon S3: Data storage for raw and processed data
- AWS Lambda: Processing small files (artists, tracks)
- AWS Glue and Apache Spark: Processing large files (features)
- AWS Glue Crawler: Automated schema detection and Data Catalog updates
- Amazon Athena: SQL queries on processed data
- Amazon QuickSight: Data visualization and dashboard creation

## Data Processing Pipeline

1. Raw data ingestion into S3
2. Data cleaning and transformation using Lambda and Glue jobs
3. Processed data storage in S3
4. Schema detection using Glue Crawler
5. Data analysis using Athena
6. Visualization using QuickSight

## Key Insights

- Music trends towards higher danceability and energy over time
- Decrease in acousticness, reflecting a shift towards electronic music
- Friday is the most popular day for music releases
- Songs between 3-4 minutes in length tend to be most popular

## Setup and Installation

1. Clone this repository
2. Set up an AWS account and configure your credentials
3. Deploy the AWS resources using the provided CloudFormation template
4. Upload the Spotify dataset to the designated S3 bucket
5. Run the Glue jobs and Lambda functions to process the data
6. Use Athena to query the processed data
7. Create visualizations in QuickSight using the Athena data source
