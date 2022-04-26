# Databricks notebook source
pip install https://pypi.python.org/packages/source/P/PrettyTable/prettytable-0.7.2.tar.bz2

# COMMAND ----------

import requests
import json
import prettytable
import os
headers = {'Content-type': 'application/json'}
# APU000074712 is the series id for gasoline price year over year https://download.bls.gov/pub/time.series/ap/ap.data.2.Gasoline
# CUSR0000SA0 is the series id for all items year over year https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems
data = json.dumps({"seriesid": ['APU000074712','CUSR0000SA0'],"startyear":"1988", "endyear":"2014"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
#print(p.text)
json_data = json.loads(p.text)
#print(json_data)

#print(os.getcwd())
for series in json_data['Results']['series']:
    x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes=""
        for footnote in item['footnotes']:
            if footnote:
                footnotes = footnotes + footnote['text'] + ','
        if 'M01' <= period <= 'M12':
            x.add_row([seriesId,year,period,value,footnotes[0:-1]])
    output = open(seriesId + '.txt','w')
    #print(seriesId)
    output.write (x.get_string())
    dbutils.fs.put("dbfs:/FileStore/my-stuff/" + seriesId + '.txt', x.get_string(), overwrite=True)
    #print(x)
    output.close()

# COMMAND ----------

#%sh ls /databricks/driver
display(dbutils.fs.ls("dbfs:/FileStore/my-stuff"))

# COMMAND ----------

#gas_price = sqlContext.read.text("dbfs:/FileStore/my-stuff/APU000074712.txt").rdd
#display(gas_price.take(5))
gas_price = spark.read.text(
  'dbfs:/FileStore/my-stuff/APU000074712.txt'
)
gas_price.printSchema()
display(gas_price.take(20))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace
gas_price_chars_removed = gas_price.select(gas_price.value, regexp_replace(gas_price.value, r'\W+', ' ').alias("split_value"))
gas_price_columns_split = gas_price_chars_removed.withColumn("seriesId", split(gas_price_chars_removed.split_value, " ").getItem(1)).withColumn("year", split(gas_price_chars_removed.split_value, " ").getItem(2)).withColumn("month", split(gas_price_chars_removed.split_value, " ").getItem(3)).withColumn("value1", split(gas_price_chars_removed.split_value, " ").getItem(4)).withColumn("value2", split(gas_price_chars_removed.split_value, " ").getItem(5)) #split the date and time into separate columns
gas_price_columns_split.printSchema()
#display(gas_price_columns_split.take(100))

gas_price_columns_split_updated_cols = gas_price_columns_split.drop(gas_price_columns_split.value)
gas_price_columns_split_updated_cols2 = gas_price_columns_split_updated_cols.drop(gas_price_columns_split_updated_cols.split_value)
#display(gas_price_columns_split_updated_cols2.take(10))

df_gas_price_null_value_removed = gas_price_columns_split_updated_cols2.na.drop()
df_gas_price_null_value_removed_new = spark.createDataFrame(df_gas_price_null_value_removed.tail(df_gas_price_null_value_removed.count()-1), df_gas_price_null_value_removed.schema)
#df_gas_price_null_value_removed_new.show()

df_gas_price_null_value_removed_new_combine_values = df_gas_price_null_value_removed_new.withColumn("actual_value", concat_ws('.', df_gas_price_null_value_removed_new.value1, df_gas_price_null_value_removed_new.value2))
df_gas_price_null_value_removed_new_combine_values.show()
#display(df_gas_price_null_value_removed_new_combine_values.take(5))

# COMMAND ----------

import pyspark.sql.functions as f
df_gas_price_workable = df_gas_price_null_value_removed_new_combine_values.select(df_gas_price_null_value_removed_new_combine_values.year, df_gas_price_null_value_removed_new_combine_values.actual_value.cast('float'))
#display(df_gas_price_workable.take(5))

grouped_aggregated = df_gas_price_workable.groupBy("year").agg(avg("actual_value").alias("gas price average")) #group by year, and take avg values of all the years
grouped_aggregated_sorted = grouped_aggregated.orderBy(col("year"))
display(grouped_aggregated_sorted.take(5))                                                               


