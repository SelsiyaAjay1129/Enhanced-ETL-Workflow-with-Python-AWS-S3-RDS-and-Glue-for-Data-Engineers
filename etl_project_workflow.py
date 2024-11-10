import boto3
import pandas as pd
import logging
from sqlalchemy import create_engine
import os
from botocore.exceptions import NoCredentialsError, ClientError
from sqlalchemy.exc import SQLAlchemyError

# AWS Configuration
S3_BUCKET_NAME = 'etl-capstone'  # Replace with your S3 bucket name
RDS_ENDPOINT = 'my-etl-db.cjmmqcasavj5.ap-southeast-2.rds.amazonaws.com'  # Replace with your RDS endpoint
DB_NAME = 'my-etl-db'  # Replace with your database name
DB_USER = 'admin'  # Replace with your RDS username
DB_PASSWORD = 'Ajay11Selsiya29'  # Replace with your RDS password
DB_PORT = 3306  # MySQL port (5432 for PostgreSQL)

# Configure logging
logging.basicConfig(filename='etl_pipeline.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Create boto3 client for S3
s3_client = boto3.client('s3')

# Step 1: Upload data to S3
def upload_to_s3(local_file, bucket, s3_file):
    try:
        s3_client.upload_file(local_file, bucket, s3_file)
        logging.info(f'Successfully uploaded {local_file} to {s3_file} in S3 bucket {bucket}')
    except FileNotFoundError:
        logging.error(f'The file {local_file} was not found')
    except NoCredentialsError:
        logging.error("AWS credentials not available")
    except ClientError as e:
        logging.error(f"Failed to upload {local_file} to S3: {e}")
    except Exception as e:
        logging.error(f"Unexpected error occurred during upload: {e}")

# Step 2: Download data from S3
def download_from_s3(bucket, s3_file, local_file):
    try:
        s3_client.download_file(bucket, s3_file, local_file)
        logging.info(f'Successfully downloaded {s3_file} from S3 bucket {bucket} to {local_file}')
    except NoCredentialsError:
        logging.error("AWS credentials not available")
    except ClientError as e:
        logging.error(f"Failed to download {s3_file} from S3: {e}")
    except Exception as e:
        logging.error(f"Unexpected error occurred during download: {e}")

# Step 3: Transform data (Unit conversions and cleaning)
def transform_data(df):
    # Print columns to debug if height_inches is missing
    print(df.columns)

    # Check if the necessary columns are present
    if 'height_inches' in df.columns:
        df['height_meters'] = df['height_inches'] * 0.0254  # inches to meters
    else:
        logging.error("height_inches column is missing in the dataset.")
    
    if 'weight_pounds' in df.columns:
        df['weight_kg'] = df['weight_pounds'] * 0.453592  # pounds to kilograms
    else:
        logging.error("weight_pounds column is missing in the dataset.")

    # Clean and standardize other columns as needed
    df.drop(columns=['height_inches', 'weight_pounds'], errors='ignore', inplace=True)
    logging.info('Data transformed successfully')
    
    return df

# Step 4: Load transformed data to S3
def load_to_s3(df, bucket, s3_file):
    temp_file = 'transformed_data.csv'
    df.to_csv(temp_file, index=False)
    upload_to_s3(temp_file, bucket, s3_file)
    os.remove(temp_file)
    logging.info(f'Transformed data loaded to S3 as {s3_file}')

# Step 5: Load transformed data to RDS
def load_to_rds(df):
    engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{RDS_ENDPOINT}:{DB_PORT}/{DB_NAME}')
    try:
        df.to_sql('transformed_data', engine, if_exists='replace', index=False)
        logging.info("Data loaded into RDS successfully")
    except SQLAlchemyError as e:
        logging.error(f"SQLAlchemy error: {e}")
    except Exception as e:
        logging.error(f"Failed to load data into RDS: {e}")

# ETL Pipeline Execution
def run_etl_pipeline():
    # Step 1: Upload raw data to S3
    try:
        upload_to_s3('source_file.csv', S3_BUCKET_NAME, 'raw/source_file.csv')
    except Exception as e:
        logging.error(f"Error in uploading raw data to S3: {e}")
        return

    # Step 2: Extract raw data from S3
    try:
        download_from_s3(S3_BUCKET_NAME, 'raw/source_file.csv', 'local_source_file.csv')
    except Exception as e:
        logging.error(f"Error in downloading raw data from S3: {e}")
        return

    # Step 3: Transform data
    try:
        df = pd.read_csv('local_source_file.csv')
        df_transformed = transform_data(df)
    except FileNotFoundError:
        logging.error("The source file does not exist locally after download")
        return
    except Exception as e:
        logging.error(f"Failed to load CSV file for transformation: {e}")
        return
    
    # Step 4: Load transformed data to S3
    try:
        load_to_s3(df_transformed, S3_BUCKET_NAME, 'transformed/transformed_data.csv')
    except Exception as e:
        logging.error(f"Error in loading transformed data to S3: {e}")
        return
    
    # Step 5: Load transformed data to RDS
    try:
        load_to_rds(df_transformed)
    except Exception as e:
        logging.error(f"Error in loading transformed data to RDS: {e}")
        return

    logging.info("ETL pipeline executed successfully")

# Execute the ETL pipeline
if __name__ == "__main__":
    run_etl_pipeline()
