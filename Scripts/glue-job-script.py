# Import necessary libraries from AWS Glue and PySpark
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, log, abs as spark_abs
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Initialize Glue context and job
# This is standard boilerplate code for AWS Glue jobs
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Step 1: Read Data from Glue Catalog ---
# This reads the table created by the Glue Crawler from S3
print("Reading transaction data from Glue Catalog...")
transactions_df = glueContext.create_dynamic_frame.from_catalog(
    database="fraud_detection_db",  # Name of the database created in Phase 3
    table_name="transactions"       # Name of the table created by the crawler in Phase 3
).toDF() # Convert Glue DynamicFrame to Spark DataFrame for PySpark operations

# --- Step 2: Basic Data Analysis & Feature Engineering ---
# Print basic statistics about the data
print(f"Total number of transactions: {transactions_df.count()}")
print(f"Number of fraudulent transactions (Class = 1): {transactions_df.filter(col('Class') == 1).count()}")
print(f"Number of normal transactions (Class = 0): {transactions_df.filter(col('Class') == 0).count()}")

# Check the actual schema of the DataFrame to confirm column types
print("Schema of the DataFrame (before type conversion):")
transactions_df.printSchema()

# --- NEW: Convert String Columns to Numeric ---
# Identify columns that should be numeric but might be string due to crawler inference
# The original dataset should only have numeric types. We'll convert potential string columns.
# Iterate through all columns except the target 'Class'
columns_to_check = [col_name for col_name in transactions_df.columns if col_name != 'Class']

for col_name in columns_to_check:
    # Get the current data type of the column
    current_type = dict(transactions_df.dtypes)[col_name]
    print(f"Processing column: {col_name}, Current Type: {current_type}")

    # If the column is of type 'string', attempt to cast it to 'double'
    if current_type == 'string':
        print(f"  Converting {col_name} from string to double...")
        # Cast the column to double. Non-numeric strings will become null.
        transactions_df = transactions_df.withColumn(col_name, col(col_name).cast("double"))
    # If the column is already numeric (double, int, etc.), leave it as is.

print("Schema of the DataFrame (after type conversion attempts):")
transactions_df.printSchema()

# --- Continue with Feature Engineering ---
# Example 1: Log of the transaction amount (helps with skewed amount data)
# Note: Amount is now confirmed to be numeric
transactions_df = transactions_df.withColumn(
    "amount_log",
    when(col("Amount").isNotNull() & (col("Amount") > 0), log(col("Amount"))).otherwise(0.0)
)

# Example 2: A simple time-based feature (is it around a typical weekend time? - Just an example)
# The 'Time' column was read as a STRUCT. We need to extract the 'long' value into a new simple column.
# Let's create a new column 'Time_seconds' containing just the numeric value from Time.long
transactions_df = transactions_df.withColumn("Time_seconds", col("Time.long"))

# Now create the 'is_weekend_approx' feature using the new simple 'Time_seconds' column
transactions_df = transactions_df.withColumn(
    "is_weekend_approx",
    when((col("Time_seconds") % (24*3600)) > (5*24*3600), 1.0).otherwise(0.0) # Use the simple numeric column
)

# Drop the original 'Time' STRUCT column as it's not suitable for VectorAssembler
# Keep the new 'Time_seconds' column instead
transactions_df = transactions_df.drop("Time")

print("Schema of the DataFrame (after flattening Time and dropping original STRUCT):")
transactions_df.printSchema()

# --- Step 3: Prepare Features for Machine Learning ---
# Define the list of columns to be used as features for the model
# Exclude the target variable 'Class'
# Use the new 'Time_seconds' column instead of the old STRUCT 'Time'
feature_columns = [col_name for col_name in transactions_df.columns if col_name not in ['Class']]

print(f"Feature columns being used (after type conversion): {feature_columns}")

# Check the types of the selected feature columns again
print("Types of feature columns:")
for fc in feature_columns:
    print(f"  {fc}: {dict(transactions_df.dtypes)[fc]}")

# Assemble the feature columns into a single vector column named 'features'
# This is required by most ML algorithms in PySpark MLlib
# Now that all features should be numeric, VectorAssembler should work.
# ADD handleInvalid='skip' to handle potential nulls created during type conversion or inherent in data
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
assembled_df = assembler.transform(transactions_df)

# Check the schema after assembling to confirm features column exists
print("Schema after Vector Assembling (handleInvalid='skip'):")
assembled_df.printSchema()
# Also check the count to see if any rows were dropped due to nulls
print(f"Number of rows after Vector Assembling: {assembled_df.count()}")

# Standardize the features (mean=0, std=1) - often improves model performance
# StandardScaler might also need to handle potential nulls in the features column if any slipped through
# It usually handles nulls in the 'scaled_features' output by creating nulls there too, which is often okay for training.
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(assembled_df) # Fit the scaler on the assembled data (which may have fewer rows now)
scaled_df = scaler_model.transform(assembled_df) # Apply the scaling transformation

# --- Step 4: Split Data for Training and Testing ---
# Split the data into training (80%) and testing (20%) sets
# Note: The split happens after potential rows might have been dropped by VectorAssembler
train_df, test_df = scaled_df.randomSplit([0.8, 0.2], seed=42) # Use a seed for reproducible results

print(f"Training set size (after potential null drops): {train_df.count()}")
print(f"Test set size (after potential null drops): {test_df.count()}")

# --- Step 5: Train the Machine Learning Model (Random Forest) ---
# Initialize the Random Forest Classifier
# labelCol: The column containing the target variable (0 or 1)
# featuresCol: The column containing the assembled features
# numTrees: Number of trees in the forest (increasing can improve performance but takes longer)
# maxDepth: Maximum depth of each tree (controls complexity)
rf = RandomForestClassifier(
    labelCol="class",          # Use the lowercase name 'class' as shown in the error
    featuresCol="scaled_features",
    numTrees=100,  # Number of trees in the forest
    maxDepth=10    # Maximum depth of each tree
)

print("Training the Random Forest model...")
# Train the model using the training dataset
model = rf.fit(train_df)

# --- Step 6: Make Predictions on the Test Set ---
print("Making predictions on the test set...")
# Apply the trained model to the test set to get predictions
predictions = model.transform(test_df)

# --- Step 7: Evaluate Model Performance (Basic Metrics) ---
# Calculate simple metrics to understand the model's performance
total_predictions = predictions.count()
correct_predictions = predictions.filter(
    col("prediction") == col("class") # Count rows where prediction matches actual label (use lowercase 'class')
).count()
accuracy = (correct_predictions / total_predictions) * 100

# Count how many actual frauds were correctly identified
frauds_in_test = predictions.filter(col("class") == 1).count() # Use lowercase 'class'
correctly_detected_frauds = predictions.filter(
    (col("class") == 1) & (col("prediction") == 1) # Actual fraud AND predicted fraud (use lowercase 'class')
).count()

print(f"--- Model Performance Metrics ---")
print(f"Overall Accuracy on Test Set: {accuracy:.2f}%")
print(f"Total Transactions in Test Set: {total_predictions}")
print(f"Actual Fraudulent Transactions in Test Set: {frauds_in_test}")
print(f"Correctly Detected Fraudulent Transactions: {correctly_detected_frauds}")
# Calculate Precision and Recall (basic versions)
if correctly_detected_frauds > 0:
    precision = (correctly_detected_frauds / predictions.filter(col("prediction") == 1).count()) * 100
    recall = (correctly_detected_frauds / frauds_in_test) * 100
    print(f"Estimated Precision (Positive Predictive Value): {precision:.2f}%")
    print(f"Estimated Recall (Sensitivity): {recall:.2f}%")
else:
    print("Precision and Recall: Cannot calculate (no frauds predicted or no frauds in test set)")
print(f"----------------------------------")

# --- Step 8: Save Results to S3 ---
print("Saving predictions and metrics to S3...")

# --- NEW: Cast columns to ensure consistent types before writing ---
# Select the required columns, EXCLUDING the complex 'probability' column for CSV
predictions_to_save = predictions.select("Time_seconds", "Amount", "class", "prediction") # Excluded 'probability'

# Cast numeric columns to DoubleType to ensure consistency
predictions_to_save = predictions_to_save.withColumn("Time_seconds", col("Time_seconds").cast("double")) \
                                         .withColumn("Amount", col("Amount").cast("double")) \
                                         .withColumn("class", col("class").cast("double")) \
                                         .withColumn("prediction", col("prediction").cast("double"))

# --- Save Predictions as CSV (Excluding 'probability') ---
# IMPORTANT: Replace 'your-name-fraud-model-output' with your actual S3 bucket name
predictions_to_save.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://your-name-fraud-model-output/predictions_csv/") # <--- UPDATE THIS BUCKET NAME

# Create a simple DataFrame for model metrics and save it to S3
# Ensure metric values are doubles if they aren't already (they should be from calculations)
metrics_data = [
    ("Accuracy", float(accuracy)), # Ensure it's a float/double
    ("Total Transactions Processed", float(total_predictions)), # Ensure it's a float/double
    ("Actual Frauds in Test Set", float(frauds_in_test)), # Ensure it's a float/double
    ("Frauds Correctly Detected", float(correctly_detected_frauds)), # Ensure it's a float/double
    ("Precision_Estimate", float(precision) if 'precision' in locals() else 0.0), # Ensure it's a float/double
    ("Recall_Estimate", float(recall) if 'recall' in locals() else 0.0) # Ensure it's a float/double
]
metrics_df = spark.createDataFrame(metrics_data, ["metric", "value"])

# Save metrics to S3 in CSV format
metrics_df.write.mode("overwrite").csv("s3://your-name-fraud-model-output/metrics/") # <--- UPDATE THIS BUCKET NAME

print("Fraud Detection Pipeline Completed Successfully!")

# Commit the job to AWS Glue to signal completion
job.commit()
