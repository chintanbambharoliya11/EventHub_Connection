# Databricks notebook source
# MAGIC %scala
# MAGIC print('h')

# COMMAND ----------

# MAGIC %scala
# MAGIC //Define dataframe
# MAGIC val rates = 
# MAGIC     spark
# MAGIC     .readStream
# MAGIC     .format("rate")
# MAGIC     .option("rowsPerSecond",10)
# MAGIC     .load

# COMMAND ----------

# MAGIC %scala
# MAGIC //Import login module
# MAGIC import kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
# MAGIC val TOPIC = "demo-test"
# MAGIC 
# MAGIC // NEW VALUE, REPLACE EVENTHUBSNAMESPACE WITH YOUR OWN VALUE
# MAGIC val BOOTSTRAP_SERVERS = "demo-kafka.servicebus.windows.net:9093"
# MAGIC 
# MAGIC // NEW VALUE, REPLACE EVENTHUBSNAMESPACE, SECRETKEYNAME, SECRETKEYVALUE WITH YOUR OWN VALUES
# MAGIC val EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://demo-kafka.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=1+FK3hBa+/DQqE/Ua2kefUsBX1U9KqbN1ZIYbSF6vbI=\";"
# MAGIC 
# MAGIC 
# MAGIC //Write df to EventHubs using Spark's Kafka connector
# MAGIC rates
# MAGIC     .select($"timestamp".alias("key"), $"value")
# MAGIC     .selectExpr("CAST(key as STRING)", "CAST(value as STRING)")
# MAGIC     .writeStream
# MAGIC     .format("kafka")
# MAGIC     .option("topic", TOPIC)
# MAGIC     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
# MAGIC     .option("kafka.sasl.mechanism", "PLAIN")
# MAGIC     .option("kafka.security.protocol", "SASL_SSL")
# MAGIC     .option("kafka.sasl.jaas.config", EH_SASL)
# MAGIC     .option("checkpointLocation", "/ratecheckpoint")
# MAGIC     .start()

# COMMAND ----------



# COMMAND ----------

import pandas as pd
import requests
import json
import time

headers = {
          "Accept" : "application/json",
          "Content-Type" : "application/json",
          }
r = requests.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=inr&order=market_cap_desc&per_page=100&page=1&sparkline=false", headers = headers)
data=r.json()
    
id_name=[]
name=[]
currentprice=[]
market_cap_rank=[]
date=[]
for market in data:
    id_name.append(market["id"])
    name.append(market["name"])
    currentprice.append(market["current_price"])
    market_cap_rank.append(market["market_cap_rank"])
    date.append(market["last_updated"])   

market_dict={
          "id":id_name,
          "currency_name":name,
          "currentprice":currentprice,
          "Market_Rank":market_cap_rank,
          "Date":date
      }
market_df = pd.DataFrame(market_dict,columns=["id","currency_name","currentprice","Market_Rank","Date"])

# COMMAND ----------

# MAGIC %scala
# MAGIC val df= spark.readstream.data

# COMMAND ----------

market_df.display()

# COMMAND ----------

    def refresh(self):
        callers_local_vars = self.caller_stack.f_locals.items()
        refreshed_names = []
        for var_name, var_val in callers_local_vars:
            for ix, name in enumerate([pgdf.name for pgdf in self.store.data]):
                if var_name == name:
                    none_found_flag = False
                    self.store.remove_dataframe(var_name)
                    self.store.add_dataframe(var_val, name=var_name)
                    refreshed_names.append(var_name)
        if not refreshed_names:
            print("No matching DataFrames found to refresh")
        else:
            print(f"Refreshed {', '.join(refreshed_names)}")

# COMMAND ----------

import pandas as pd
import requests
import json
import time
while True:
    headers = {
              "Accept" : "application/json",
              "Content-Type" : "application/json",
              }
    r = requests.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=inr&order=market_cap_desc&per_page=100&page=1&sparkline=false", headers = headers)
    data=r.json()
    time.sleep(10)
    print(data)

# COMMAND ----------


