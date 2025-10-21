-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Predictive Maintenance Inference Pipeline - ML Model Serving
-- MAGIC
-- MAGIC This notebook implements the ML inference pipeline for Navy predictive maintenance using Delta Live Tables.
-- MAGIC It reads from the silver layer tables created by the ingestion pipeline and performs real-time predictions
-- MAGIC using model serving endpoints.
-- MAGIC
-- MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
-- MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
-- MAGIC   <div style="height: 300px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
-- MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
-- MAGIC       AI
-- MAGIC     </div>
-- MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">Real-time predictive maintenance using model serving endpoints</div>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br>
-- MAGIC
-- MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> ML Inference Pipeline Architecture
-- MAGIC
-- MAGIC
-- MAGIC * **Real-time predictions**: Using ai_query() to call model serving endpoints<br>
-- MAGIC * **Scalable inference**: Serverless model serving with automatic scaling<br>  
-- MAGIC * **Business intelligence ready**: Gold layer tables for downstream analytics<br>
-- MAGIC * **Data quality**: Built-in constraints and monitoring<br><br>
-- MAGIC
-- MAGIC This pipeline depends on silver layer tables from the ingestion pipeline and produces gold layer business datasets.
-- MAGIC <br style="clear: both">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # ML Inference with Model Serving Endpoints
-- MAGIC
-- MAGIC <img style="float: right" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-maintenance-1.png" />
-- MAGIC
-- MAGIC In this notebook, we'll work as ML Engineers to implement real-time inference for our predictive maintenance use case.
-- MAGIC We'll use Databricks Model Serving to get predictions from our trained models.
-- MAGIC
-- MAGIC ## Model Serving: Scalable, Real-time ML Inference
-- MAGIC
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>Serverless Inference</strong> <br/>
-- MAGIC       Auto-scaling model endpoints that handle traffic spikes automatically
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>Real-time Processing</strong> <br/>
-- MAGIC       Low-latency predictions integrated directly into data pipelines
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>Production Ready</strong> <br/>
-- MAGIC       Built-in monitoring, versioning, and A/B testing capabilities
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>Unified Platform</strong> <br/>
-- MAGIC       Seamless integration between training, serving, and data pipelines
-- MAGIC     </p>
-- MAGIC </div>
-- MAGIC </div>
-- MAGIC
-- MAGIC <br style="clear:both">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Building ML Inference Pipeline with Model Serving
-- MAGIC
-- MAGIC This pipeline implements real-time predictive maintenance inference using our trained model deployed on Databricks Model Serving.
-- MAGIC
-- MAGIC ### Data Flow:
-- MAGIC
-- MAGIC 1. **Read Silver Data**: Consume cleaned sensor data and metadata from ingestion pipeline
-- MAGIC 2. **Feature Engineering**: Prepare features for model inference  
-- MAGIC 3. **Model Inference**: Call model serving endpoint using ai_query()
-- MAGIC 4. **Business Logic**: Join predictions with maintenance recommendations
-- MAGIC 5. **Gold Layer**: Create business-ready dataset for analytics and dashboards
-- MAGIC
-- MAGIC ### Dependencies:
-- MAGIC
-- MAGIC This pipeline depends on silver layer tables from the ingestion pipeline:
-- MAGIC * **sensor_silver**: Hourly aggregated sensor statistics
-- MAGIC * **ship_meta_silver**: Ship to turbine metadata mapping  
-- MAGIC * **maintenance_actions_silver**: Maintenance action recommendations
-- MAGIC
-- MAGIC Let's implement the inference flow:

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 1/ Real-time Model Inference
-- MAGIC
-- MAGIC <div><img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-4.png" width="700px" style="float: right"/></div>
-- MAGIC
-- MAGIC We'll use our trained predictive maintenance model deployed as a serving endpoint to perform real-time inference.
-- MAGIC
-- MAGIC The ai_query() function allows us to call model serving endpoints directly from SQL, making it easy to integrate ML predictions into our data pipeline.
-- MAGIC
-- MAGIC Key features:
-- MAGIC - **Real-time predictions**: Low-latency inference using serverless endpoints
-- MAGIC - **Automatic scaling**: Handles varying loads automatically  
-- MAGIC - **Version management**: Easy model updates without pipeline changes
-- MAGIC - **Monitoring**: Built-in tracking of prediction performance
-- MAGIC

-- COMMAND ----------


CREATE OR REFRESH MATERIALIZED VIEW current_status_predictions (
  CONSTRAINT turbine_id_valid EXPECT (turbine_id IS not NULL) ON VIOLATION DROP ROW,
  CONSTRAINT timestamp_valid EXPECT (hourly_timestamp IS not NULL) ON VIOLATION DROP ROW,
  CONSTRAINT prediction_valid EXPECT (prediction IS not NULL) ON VIOLATION DROP ROW
)
COMMENT "Real-time turbine status predictions using model serving endpoint - includes latest sensor metrics and ML predictions"
AS
WITH latest_metrics AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY turbine_id, hourly_timestamp ORDER BY hourly_timestamp DESC) AS row_number 
  FROM ${catalog}.${db}.sensor_silver s
  INNER JOIN ${catalog}.${db}.ship_meta_silver t USING (turbine_id)
)
SELECT * EXCEPT(m.row_number), 
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
    ) AS prediction
  FROM latest_metrics m
  WHERE m.row_number=1

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## 2/ Business Intelligence Gold Layer
-- MAGIC
-- MAGIC Our final gold layer combines ML predictions with business logic to create actionable insights.
-- MAGIC
-- MAGIC This table joins predictions with maintenance action recommendations, providing a complete view for:
-- MAGIC - **Operations teams**: Real-time turbine health monitoring
-- MAGIC - **Maintenance crews**: Actionable recommendations and priorities
-- MAGIC - **Business analysts**: Performance metrics and trend analysis
-- MAGIC - **Executive dashboards**: High-level KPIs and alerts
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW ship_current_status_gold (
  CONSTRAINT turbine_id_valid EXPECT (turbine_id IS not NULL) ON VIOLATION DROP ROW,
  CONSTRAINT prediction_valid EXPECT (prediction IS not NULL) ON VIOLATION DROP ROW
)
COMMENT "Gold layer: Complete turbine status with predictions and maintenance recommendations - ready for business intelligence and dashboards"
AS
SELECT * EXCEPT(_rescued_data, m.fault) 
FROM current_status_predictions p
LEFT JOIN ${catalog}.${db}.maintenance_actions_silver m ON p.prediction = m.fault

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC Our ML Inference Pipeline is now complete and provides real-time predictive maintenance capabilities.
-- MAGIC
-- MAGIC ### Key Benefits:
-- MAGIC
-- MAGIC * **Real-time Insights**: Immediate detection of potential turbine issues
-- MAGIC * **Scalable Architecture**: Serverless model serving handles any load
-- MAGIC * **Business Ready**: Gold layer provides actionable recommendations
-- MAGIC * **Integrated Workflow**: Seamless connection between data engineering and ML
-- MAGIC
-- MAGIC ### Next Steps:
-- MAGIC
-- MAGIC * Deploy this pipeline alongside the ingestion pipeline
-- MAGIC * Configure scheduling and dependencies 
-- MAGIC * Set up monitoring and alerting
-- MAGIC * Build dashboards using the gold layer data
-- MAGIC
-- MAGIC The inference pipeline integrates seamlessly with our existing data architecture while providing powerful ML capabilities for proactive maintenance decisions.
-- MAGIC
