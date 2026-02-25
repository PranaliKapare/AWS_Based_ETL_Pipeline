import pandas as pd
import boto3
from sqlalchemy import create_engine

# -----------------------------
# AWS CONFIGURATION
# -----------------------------
AWS_ACCESS_KEY = "YOUR_ACCESS_KEY"
AWS_SECRET_KEY = "YOUR_SECRET_KEY"
RAW_BUCKET = "etl-raw-data-pranali"
CLEAN_BUCKET = "etl-cleaned-data-pranali"
FILE_KEY = "sales_data.csv"

RDS_ENDPOINT = "YOUR_RDS_ENDPOINT"
DB_USER = "admin"
DB_PASSWORD = "yourpassword"
DB_NAME = "etl_project"

# -----------------------------
# CONNECT TO S3
# -----------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# -----------------------------
# EXTRACT
# -----------------------------
response = s3.get_object(Bucket=RAW_BUCKET, Key=FILE_KEY)
df = pd.read_csv(response["Body"])

print("Raw Data:")
print(df.head())

# -----------------------------
# TRANSFORM
# -----------------------------
# Remove missing values
df = df.dropna()

# Standardize column names
df.columns = [col.lower().replace(" ", "_") for col in df.columns]

# Add new column (total_amount)
df["total_amount"] = df["quantity"] * df["price"]

print("Cleaned Data:")
print(df.head())

# Save cleaned file locally
df.to_csv("cleaned_sales_data.csv", index=False)

# Upload cleaned file to S3
s3.upload_file("cleaned_sales_data.csv", CLEAN_BUCKET, "cleaned_sales_data.csv")

# -----------------------------
# LOAD INTO RDS
# -----------------------------
engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{RDS_ENDPOINT}/{DB_NAME}"
)

df.to_sql("sales_data", con=engine, if_exists="replace", index=False)

print("ETL Pipeline Completed Successfully!")
