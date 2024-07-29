# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/supply-chain-optimization. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/supply-chain-distribution-optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC # Introduction
# MAGIC
# MAGIC
# MAGIC **Situation**:
# MAGIC The situation that we model is that of a manufacturer of power tools. The manufacturer operates 3 plants and delivers a set of 30 product SKUs to 5 distribution centers. Each distribution center is assigned to a set of between 40 and 60 hardware stores. All these parameters are treated as variables such that the pattern of the code may be scaled (see later). Each store has a demand series for each of the products. 
# MAGIC
# MAGIC
# MAGIC **The following are given**:
# MAGIC - the demand series for each product in each hardware store
# MAGIC - a mapping table that uniquely assigns each distribution center to a hardware store. This is a simplification as it is possible that one hardware store obtains products from different distribution centers.
# MAGIC - a table that assigns the costs of shipping a product from each manufacturing plant to each distribution center
# MAGIC - a table of the maximum quantities of product that can be produced and shipped from each plant to each of the distribution centers
# MAGIC
# MAGIC
# MAGIC **We proceed in 2 steps**:
# MAGIC - *Demand Forecasting*: The demand forecast is first estimated one week ahead. Aggregation yields the demand for each distribution center: 
# MAGIC   - For the demand series for each product within each store we generate a one-week-ahead forecast
# MAGIC   - For the distribution center, we derive next week's estimated demand for each product
# MAGIC - *Minimization of transportation costs*: From the cost and constraints tables of producing by and shipping from a plant to a distribution center we derive cost-optimal transportation.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-industry-solutions/supply-chain-optimization/main/pictures/Plant_DistributionCenter_Store_Prouct_2.png" width=70%>

# COMMAND ----------

# MAGIC %md
# MAGIC # Transport Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC *Prerequisite: Make sure to run 02_Demand_Forecast_From_Predictions before running this notebook.*
# MAGIC
# MAGIC In this notebook we solve the LP to optimize transport costs when shipping products from the plants to the distribution centers. Furthermore, we show how to scale to hundreds of thousands of products.
# MAGIC
# MAGIC Key highlights for this notebook:
# MAGIC - Use Databricks' collaborative and interactive notebook environment to find optimization procedure
# MAGIC - Pandas UDFs (user-defined functions) can take your single-node data science code, and distribute it across multiple nodes 
# MAGIC
# MAGIC More precisely we solve the following optimzation problem for each product.
# MAGIC
# MAGIC *Mathematical goal:*
# MAGIC We have a set of manufacturing plants that distribute products to a set of distribution centers. The goal is to minimize overall shipment costs, i.e. we minimize w.r.t. quantities: <br/>
# MAGIC cost_of_plant_1_to_distribution_center_1 * quantity_shipped_of_plant_1_to_distribution_center_1 <br/> \+ … \+ <br/>
# MAGIC cost_of_plant_1_to_distribution_center_n * quantity_shipped_of_plant_n_to_distribution_center_m 
# MAGIC
# MAGIC *Mathematical constraints:*
# MAGIC - Quantities shipped must be zero or positive integers
# MAGIC - The sum of products shipped from one manufacturing plant does not exceed its maximum supply 
# MAGIC - The sum of products shipped to each distribution center meets at least the demand forecasted 

# COMMAND ----------

# The pulp library is used for solving the LP problem
# We used this documentation for developemnt of this notebook
# https://coin-or.github.io/pulp/CaseStudies/a_transportation_problem.html
%pip install pulp

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../_resources/05-generate-supply-data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining and solving the LP

# COMMAND ----------

import os
import datetime as dt
import re
import numpy as np
import pandas as pd

import pulp

import pyspark.sql.functions as f
from pyspark.sql.types import *

lp_table = spark.read.table(f'{catalog}.{db}.lp_table')
display(lp_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define optimization function and output schema

# COMMAND ----------

# Result of optimzation schema
res_schema = StructType(
  [
    StructField('type', StringType()),
    StructField('stock_location', StringType()),
    StructField('ship', StringType()),
    StructField('qty_shipped', IntegerType())
  ]
)

# COMMAND ----------

# Define a function that solves the LP for one product

def transport_optimization(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf['stock_location'] = pdf['stock_location'].str.replace(' ', '_')

    #Plants list, this defines the order of other data structures related to plants
    plants_lst = sorted(pdf["stock_location"].unique().tolist())
    # plants_lst = [s.replace(' ', '_') for s in plants_lst] 

    # Distribution center list, this defines the order of other data structures related to distribution centers
    p = re.compile('^Cost_.+')
    distribution_centers_lst = sorted([ s[5:] for s in list(pdf.columns.values) if p.search(s) ])

    # Define all possible routes
    routes = [(p, d) for p in plants_lst for d in distribution_centers_lst]

    # Create a dictionary which contains the LP variables. The reference keys to the dictionary are the plant's name, then the distribution center's name and the
    # data is Route_Tuple. (e.g. ["plant_1"]["distribution_center_1"]: Route_plant_1_distribution_center_1). Set lower limit to zero, upper limit to None and
    # define as integers
    vars = pulp.LpVariable.dicts("Route", (plants_lst, distribution_centers_lst), 0, None, pulp.LpInteger)

    # Subset other lookup tables
    ss_prod = pdf[ "type" ][0]

    # Costs, order of distribution centers and plants matter
    transport_cost_table_pdf = pdf.filter(regex="^Cost_.+|^stock_location$")
    transport_cost_table_pdf = (transport_cost_table_pdf.
                                rename(columns=lambda x: re.sub("^Cost_USS","USS",x)).
                                set_index("stock_location").
                                reindex(plants_lst, axis=0).
                                reindex(distribution_centers_lst, axis=1)
                                )
    costs = pulp.makeDict([plants_lst, distribution_centers_lst], transport_cost_table_pdf.values.tolist(), 0)

    # Supply, order of plants matters
    plant_supply_pdf = (pdf.filter(regex="^Supply.+$").
                        drop_duplicates().
                        rename(columns=lambda x: re.sub("^Supply_FLC","FLC",x)).
                        reindex(plants_lst, axis=1))


    supply = plant_supply_pdf.to_dict("records")[0]

    # Demand, order of distribution centers matters


    distribution_center_demand_pdf =  (pdf.
                        filter(regex="^Demand_USS.+$").
                        drop_duplicates().
                        rename(columns=lambda x: re.sub("^Demand_USS","USS",x))
                        )
    new_list = [name for name in distribution_center_demand_pdf.columns if name in distribution_centers_lst]
    distribution_center_demand_pdf = distribution_center_demand_pdf.reindex(new_list, axis=1)

    demand = distribution_center_demand_pdf.to_dict("records")[0]

    # Create the 'prob' variable to contain the problem data
    prob = pulp.LpProblem("Product_Distribution_Problem", pulp.LpMinimize)

    # Add objective function to 'prob' first
    prob += (
        pulp.lpSum([vars[p][d] * costs[p][d] for (p, d) in routes]),
        "Sum_of_Transporting_Costs",
    )

    # Add supply restrictions
    for p in plants_lst:
        prob += (
            pulp.lpSum([vars[p][d] for d in distribution_centers_lst]) <= supply[p],
            f"Sum_of_Products_out_of_Plant_{p}",
        )

    # Add demand restrictions
    for d in distribution_centers_lst:
        prob += (
            pulp.lpSum([vars[p][d] for p in plants_lst]) >= demand[d],
            f"Sum_of_Products_into_Distibution_Center{d}",
        )

    # # The problem is solved using PuLP's choice of Solver
    prob.solve()

    # # Write output fot the product
    if (pulp.LpStatus[prob.status] == "Optimal"):
        name_lst = [ ]
        value_lst = [ ]
        for v in prob.variables():
            name_lst.append(v.name) 
            value_lst.append(v.varValue)
            res = pd.DataFrame(data={'name': name_lst, 'qty_shipped': value_lst})
            res[ "qty_shipped" ] = res[ "qty_shipped" ].astype("int")
            res[ "stock_location" ] =  res[ "name" ].str.extract(r'(FLC_.*?)(?=_USS)')
            res[ "ship" ] =  res[ "name" ].str.extract(r'(USS_.+)')
            res[ "type" ] = ss_prod
            res = res.drop("name", axis = 1)
            res = res[[ "type", "stock_location", "ship", "qty_shipped"]]
    else:
        res = pd.DataFrame(data= {  "type" : [ ss_prod ] , "stock_location" : [ None ], "ship" : [ None ], "qty_shipped" : [ None ]})
    return res

# COMMAND ----------

# Test the function
product_selection = "Seal"
pdf = lp_table.filter(f.col("type")==product_selection).toPandas()
transport_optimization(pdf)

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
n_tasks = lp_table.select("type").distinct().count()

optimal_transport_df = (
  lp_table
  .repartition(n_tasks, "type")
  .groupBy("type")
  .applyInPandas(transport_optimization, schema=res_schema)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Catalog

# COMMAND ----------

optimal_transport_df.createOrReplaceTempView('temp')
spark.sql(
  f"""
  CREATE OR REPLACE TABLE {catalog}.{db}.shipment_recommendations_navy AS SELECT * FROM temp
  """
  )

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{db}.shipment_recommendations_navy WHERE qty_shipped > 0"))

# COMMAND ----------

# MAGIC %md 
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | pulp                                 | A python Linear Programming API      | https://github.com/coin-or/pulp/blob/master/LICENSE        | https://github.com/coin-or/pulp                      |

# COMMAND ----------


