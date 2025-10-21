# Databricks notebook source
# MAGIC %md 
# MAGIC # Model Serving Endpoint Validation
# MAGIC 
# MAGIC This notebook validates that the `navy_predictive_maintenance` model serving endpoint is available 
# MAGIC and functioning correctly by testing it with sample data from the turbine_training_dataset.

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

import json
import requests
import os
import time
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Set up catalog and database
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {db}")

print(f"Validating model endpoint for catalog: {catalog}, schema: {db}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Get Sample Data from turbine_training_dataset

# COMMAND ----------

# Get sample data from the training dataset - matching the expected model input format
sample_data = spark.sql(f"""
  SELECT 
    hourly_timestamp,
    avg_energy,
    std_sensor_A,
    std_sensor_B, 
    std_sensor_C,
    std_sensor_D,
    std_sensor_E,
    std_sensor_F,
    percentiles_sensor_A,
    percentiles_sensor_B,
    percentiles_sensor_C,
    percentiles_sensor_D,
    percentiles_sensor_E,
    percentiles_sensor_F
  FROM {catalog}.{db}.turbine_training_dataset
  WHERE hourly_timestamp IS NOT NULL 
    AND avg_energy IS NOT NULL
    AND std_sensor_A IS NOT NULL
  LIMIT 5
""").collect()

if len(sample_data) == 0:
    raise Exception("No sample data found in turbine_training_dataset. Cannot validate endpoint.")

print(f"Retrieved {len(sample_data)} sample records for validation")
display(spark.sql(f"SELECT * FROM {catalog}.{db}.turbine_training_dataset LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Model Serving Endpoint

# COMMAND ----------

def validate_model_endpoint(endpoint_name, sample_records):
    """
    Validate the model serving endpoint with sample data using the same format as ai_query()
    """
    # Get workspace URL dynamically
    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    except:
        # Fallback for older Databricks versions
        workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    
    # Get Databricks token with proper fallback chain
    token = None
    try:
        token = dbutils.secrets.get(scope="dbdemos", key="sp_token")
        print("Using token from dbdemos secret scope")
    except:
        try:
            token = os.environ.get("DATABRICKS_TOKEN")
            if token:
                print("Using token from environment variable")
        except:
            pass
    
    if not token:
        try:
            # Use current user context token as last resort
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
            print("Using context token")
        except:
            raise Exception("No valid Databricks token found. Please set DATABRICKS_TOKEN env var or configure secrets.")
    
    url = f"https://{workspace_url}/serving-endpoints/{endpoint_name}/invocations"
    headers = {
        'Authorization': f'Bearer {token}', 
        'Content-Type': 'application/json'
    }
    
    successful_predictions = 0
    failed_predictions = 0
    
    print(f"Testing endpoint: {url}")
    
    for i, record in enumerate(sample_records):
        try:
            # Prepare the request payload matching ai_query() format
            # Convert percentile arrays to proper format
            def safe_percentiles(percentiles):
                if percentiles is None:
                    return [0.0, 0.0, 0.0, 0.0, 0.0]
                if isinstance(percentiles, list):
                    return [float(x) for x in percentiles]
                # If it's a string representation, try to parse it
                try:
                    import ast
                    parsed = ast.literal_eval(str(percentiles))
                    return [float(x) for x in parsed] if isinstance(parsed, list) else [0.0, 0.0, 0.0, 0.0, 0.0]
                except:
                    return [0.0, 0.0, 0.0, 0.0, 0.0]
            
            # Prepare payload in the format expected by model serving endpoint
            payload = {
                "inputs": [
                    {
                        "hourly_timestamp": str(record.hourly_timestamp),
                        "avg_energy": float(record.avg_energy) if record.avg_energy is not None else 0.0,
                        "std_sensor_A": float(record.std_sensor_A) if record.std_sensor_A is not None else 0.0,
                        "std_sensor_B": float(record.std_sensor_B) if record.std_sensor_B is not None else 0.0,
                        "std_sensor_C": float(record.std_sensor_C) if record.std_sensor_C is not None else 0.0,
                        "std_sensor_D": float(record.std_sensor_D) if record.std_sensor_D is not None else 0.0,
                        "std_sensor_E": float(record.std_sensor_E) if record.std_sensor_E is not None else 0.0,
                        "std_sensor_F": float(record.std_sensor_F) if record.std_sensor_F is not None else 0.0,
                        "percentiles_sensor_A": safe_percentiles(record.percentiles_sensor_A),
                        "percentiles_sensor_B": safe_percentiles(record.percentiles_sensor_B),
                        "percentiles_sensor_C": safe_percentiles(record.percentiles_sensor_C),
                        "percentiles_sensor_D": safe_percentiles(record.percentiles_sensor_D),
                        "percentiles_sensor_E": safe_percentiles(record.percentiles_sensor_E),
                        "percentiles_sensor_F": safe_percentiles(record.percentiles_sensor_F)
                    }
                ]
            }
            
            # Make the request
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                prediction_result = response.json()
                print(f"✅ Sample {i+1}: Prediction successful - {prediction_result}")
                successful_predictions += 1
                
                # Validate the response format (model serving can return different formats)
                if 'predictions' not in prediction_result and 'outputs' not in prediction_result and len(prediction_result) == 0:
                    print(f"⚠️  Warning: Unexpected response format for sample {i+1}: {prediction_result}")
                else:
                    # Extract the actual prediction value for validation
                    pred_value = None
                    if 'predictions' in prediction_result:
                        pred_value = prediction_result['predictions'][0] if isinstance(prediction_result['predictions'], list) else prediction_result['predictions']
                    elif isinstance(prediction_result, list) and len(prediction_result) > 0:
                        pred_value = prediction_result[0]
                    
                    print(f"   Prediction value: {pred_value}")
                    
            else:
                print(f"❌ Sample {i+1}: HTTP {response.status_code} - {response.text}")
                failed_predictions += 1
                
        except Exception as e:
            print(f"❌ Sample {i+1}: Exception - {str(e)}")
            failed_predictions += 1
    
    return successful_predictions, failed_predictions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run Validation with Retry Logic

# COMMAND ----------

# Validate the endpoint with retry logic for warm-up
def validate_with_retry(endpoint_name, sample_data, max_retries=3, retry_delay=30):
    """
    Validate endpoint with retry logic to handle cold start/warm-up issues
    """
    print(f"Validating endpoint with up to {max_retries} attempts...")
    
    for attempt in range(1, max_retries + 1):
        print(f"\n{'='*50}")
        print(f"VALIDATION ATTEMPT {attempt}/{max_retries}")
        print(f"{'='*50}")
        
        try:
            successful, failed = validate_model_endpoint(endpoint_name, sample_data)
            
            print(f"Endpoint: {endpoint_name}")
            print(f"Successful predictions: {successful}")
            print(f"Failed predictions: {failed}")
            print(f"Total samples tested: {len(sample_data)}")
            
            # Consider validation successful if we have at least some successes
            # and the failure rate is reasonable (< 50%)
            total_tests = len(sample_data)
            success_rate = successful / total_tests if total_tests > 0 else 0
            
            if failed == 0:
                print(f"✅ All predictions succeeded on attempt {attempt}")
                return True, successful, failed, attempt
            elif success_rate >= 0.6:  # At least 60% success rate
                print(f"✅ Acceptable success rate ({success_rate:.1%}) on attempt {attempt}")
                return True, successful, failed, attempt
            elif attempt < max_retries:
                print(f"⚠️  Attempt {attempt} had {failed} failures. Retrying in {retry_delay} seconds...")
                print(f"   Success rate: {success_rate:.1%} (need ≥60% or 0 failures)")
                
                # Wait for endpoint to warm up
                time.sleep(retry_delay)
                continue
            else:
                print(f"❌ Final attempt failed with {failed} failures")
                return False, successful, failed, attempt
                
        except Exception as e:
            print(f"❌ Attempt {attempt} failed with exception: {str(e)}")
            if attempt < max_retries:
                print(f"   Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            else:
                print(f"❌ All {max_retries} attempts failed")
                return False, 0, len(sample_data), attempt
    
    return False, 0, len(sample_data), max_retries

# Run validation with retry logic
endpoint_name = "navy_predictive_maintenance"
max_retries = 3
retry_delay = 30  # seconds

validation_passed, successful, failed, attempts_used = validate_with_retry(
    endpoint_name, 
    sample_data, 
    max_retries=max_retries, 
    retry_delay=retry_delay
)

# Store results for final validation section
rest_api_passed = validation_passed

print(f"\n{'='*50}")
print(f"REST API VALIDATION SUMMARY")
print(f"{'='*50}")
print(f"Attempts used: {attempts_used}/{max_retries}")
print(f"Final successful predictions: {successful}")
print(f"Final failed predictions: {failed}")
print(f"Result: {'✅ PASSED' if rest_api_passed else '❌ FAILED'}")

if not rest_api_passed:
    error_msg = f"REST API validation FAILED after {attempts_used} attempts. Final result: {failed} failures out of {len(sample_data)} tests."
    print(f"❌ {error_msg}")
    # Don't raise exception here - let final validation section handle it

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test SQL ai_query() Function Format
# MAGIC 
# MAGIC Test that the same endpoint works with ai_query() function (used by the actual pipeline)

# COMMAND ----------

def test_ai_query_format():
    """
    Test the endpoint using ai_query() function to match exactly how it's used in the pipeline
    """
    try:
        # Test with a single record using ai_query() - matches the inference pipeline exactly
        test_query = f"""
        SELECT 
          hourly_timestamp,
          avg_energy,
          ai_query(
            endpoint => 'navy_predictive_maintenance',
            request => struct(
              hourly_timestamp,
              avg_energy,
              std_sensor_A,
              std_sensor_B,
              std_sensor_C,
              std_sensor_D,
              std_sensor_E,
              std_sensor_F,
              percentiles_sensor_A,
              percentiles_sensor_B,
              percentiles_sensor_C,
              percentiles_sensor_D,
              percentiles_sensor_E,
              percentiles_sensor_F
            ),
            returnType => 'STRING'
          ) AS ai_prediction
        FROM {catalog}.{db}.turbine_training_dataset
        LIMIT 1
        """
        
        result = spark.sql(test_query).collect()
        
        if len(result) > 0:
            prediction = result[0]['ai_prediction']
            print(f"✅ ai_query() validation successful: {prediction}")
            return True
        else:
            print("❌ ai_query() returned no results")
            return False
            
    except Exception as e:
        print(f"❌ ai_query() validation failed: {str(e)}")
        return False

# Run ai_query validation
print("Testing ai_query() format (used by actual pipeline)...")
ai_query_ok = test_ai_query_format()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Additional Health Checks

# COMMAND ----------

# Test endpoint availability with a simple health check
def check_endpoint_health(endpoint_name):
    """
    Perform a basic health check on the endpoint by checking if it exists and is ready
    """
    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    except:
        workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    
    # Get token using same logic as validation function
    token = None
    try:
        token = dbutils.secrets.get(scope="dbdemos", key="sp_token")
    except:
        try:
            token = os.environ.get("DATABRICKS_TOKEN")
        except:
            pass
    
    if not token:
        try:
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except:
            print("❌ Could not obtain authentication token for health check")
            return False
    
    # Check if endpoint exists and is ready
    api_url = f"https://{workspace_url}/api/2.0/serving-endpoints/{endpoint_name}"
    headers = {'Authorization': f'Bearer {token}'}
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        if response.status_code == 200:
            endpoint_info = response.json()
            state = endpoint_info.get('state', {}).get('ready', 'UNKNOWN')
            config_update = endpoint_info.get('state', {}).get('config_update', 'UNKNOWN')
            print(f"✅ Endpoint exists and ready state: {state}, config_update: {config_update}")
            
            # Consider endpoint healthy if it's ready or updating
            return state in ['READY', 'UPDATING'] or config_update in ['IN_PROGRESS', 'UPDATE_SUCCEEDED']
        else:
            print(f"❌ Endpoint check failed: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health check exception: {str(e)}")
        return False

# Run health check
print("Running endpoint health check...")
health_ok = check_endpoint_health(endpoint_name)

if not health_ok:
    raise Exception(f"Endpoint health check failed for {endpoint_name}")

# Final validation summary
print(f"\n{'='*60}")
print(f"FINAL VALIDATION SUMMARY")  
print(f"{'='*60}")
print(f"REST API validation: {'✅ PASSED' if rest_api_passed else '❌ FAILED'} (after {attempts_used} attempts)")
print(f"ai_query() validation: {'✅ PASSED' if ai_query_ok else '❌ FAILED'}")
print(f"Health check: {'✅ PASSED' if health_ok else '❌ FAILED'}")

# Overall validation result
if rest_api_passed and ai_query_ok and health_ok:
    print(f"\n🎉 ALL VALIDATIONS PASSED! Endpoint {endpoint_name} is ready for use in the pipeline.")
    print(f"   REST API required {attempts_used} attempt(s)")
else:
    failed_checks = []
    if not rest_api_passed:
        failed_checks.append(f"REST API calls (failed after {attempts_used} attempts)")
    if not ai_query_ok:
        failed_checks.append("ai_query() function")  
    if not health_ok:
        failed_checks.append("health check")
    
    error_msg = f"Validation FAILED. Issues with: {', '.join(failed_checks)}"
    print(f"❌ {error_msg}")
    raise Exception(error_msg)
