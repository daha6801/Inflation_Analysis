# Databricks notebook source
pip install https://pypi.python.org/packages/source/P/PrettyTable/prettytable-0.7.2.tar.bz2

# COMMAND ----------

#dbutils.fs.help("put")
dbutils.fs.rm("dbfs:/FileStore/my-stuff/APU0000747141.txt")

# COMMAND ----------

from requests.auth import HTTPBasicAuth
import requests
import json
import prettytable
import os
headers = {'Content-type': 'application/json'}
auth = HTTPBasicAuth('apikey','bf323d2b0dff40a79cf1df469caec067')
# APU000074714  - Gasoline, unleaded regular, per gallon/3.785 liters in U.S. city average, average price, not seasonally adjusted		1976	M01	2022	M03  https://download.bls.gov/pub/time.series/ap/ap.data.2.Gasoline
# CUSR0000SA0      	0000	SA0	S	R	S	1982-84=100	All items in U.S. city average, all urban consumers, seasonally adjusted		1947	M01	2022	M03  https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems
startyeardate = 1976
endyeardate = 2022
master_string = ""
for i in range(0, 5):
    i = i+1
    print(i)
    print(startyeardate)
    print(endyeardate)
    if startyeardate >= 1976 or endyeardate <= 2022:
        data = json.dumps({"seriesid": ['APU000074714', 'APU000074714'],"startyear":str(startyeardate), "endyear":str(endyeardate)})
        print(data)
        p = requests.post('https://api.bls.gov/publicAPI/v1/timeseries/data/', data=data, headers=headers, auth = auth)
        print(p.text)
        json_data = json.loads(p.text)
        startyeardate = startyeardate + 10;
        endyeardate = endyeardate + 10;
        print(startyeardate)
        print(endyeardate)
        
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
            master_string = master_string + x.get_string()
            print(i)
            output.close()
dbutils.fs.put("dbfs:/FileStore/my-stuff/" + seriesId + str(i) + '.txt', master_string, overwrite=True)

# COMMAND ----------

#%sh ls /databricks/driver
display(dbutils.fs.ls("dbfs:/FileStore/my-stuff"))

# COMMAND ----------

gas_price = spark.read.text(
  'dbfs:/FileStore/my-stuff/APU0000747145.txt'
)
#gas_price.printSchema()
display(gas_price)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace

gas_price_chars_removed = gas_price.select(gas_price.value, regexp_replace(gas_price.value, r'\W+', ' ').alias("split_value")) # remove special characters from each row and replace with space
gas_price_columns_split = gas_price_chars_removed.withColumn("seriesId", split(gas_price_chars_removed.split_value, " ").getItem(1)).withColumn("year", split(gas_price_chars_removed.split_value, " ").getItem(2)).withColumn("month", split(gas_price_chars_removed.split_value, " ").getItem(3)).withColumn("whole_value", split(gas_price_chars_removed.split_value, " ").getItem(4)).withColumn("decimal_value", split(gas_price_chars_removed.split_value, " ").getItem(5)) #split everything into it's own column
#display(gas_price_columns_split.take(100))

gas_price_columns_split_updated_cols = gas_price_columns_split.drop(gas_price_columns_split.value) #drop the original column(value column) from the dataframe
gas_price_columns_split_updated_cols2 = gas_price_columns_split_updated_cols.drop(gas_price_columns_split_updated_cols.split_value) # drop split_value column from the dataframe
#display(gas_price_columns_split_updated_cols2.take(10))

df_gas_price_null_value_removed = gas_price_columns_split_updated_cols2.na.drop() #drop any column that doesn't have any values
#df_gas_price_null_value_removed.show()
df_gas_price_null_value_removed_new = spark.createDataFrame(df_gas_price_null_value_removed.tail(df_gas_price_null_value_removed.count()-1), df_gas_price_null_value_removed.schema) #remove the first column of the dataframe because it contins strings
#df_gas_price_null_value_removed_new.show()

df_gas_price_null_value_removed_new_combine_values = df_gas_price_null_value_removed_new.withColumn("actual_value", concat_ws('.', df_gas_price_null_value_removed_new.whole_value, df_gas_price_null_value_removed_new.decimal_value)) # merge the whole_value and decimal_value to form the original price
df_gas_price_null_value_removed_new_combine_values.show()


# COMMAND ----------

import pyspark.sql.functions as f
df_gas_price_workable = df_gas_price_null_value_removed_new_combine_values.select(df_gas_price_null_value_removed_new_combine_values.year, df_gas_price_null_value_removed_new_combine_values.actual_value.cast('float')) #seclect only the year and the price of gasoline
#display(df_gas_price_workable.take(5))

grouped_aggregated = df_gas_price_workable.groupBy("year").agg(avg("actual_value").alias("gas price average")) #group by year, and take avg values of all the years
grouped_aggregated_sorted = grouped_aggregated.orderBy(col("year")) # sort by year
display(grouped_aggregated_sorted)                                                               



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col
W = Window.orderBy('year')

percent_change = grouped_aggregated_sorted.withColumn('gas price average-1',f.lag(grouped_aggregated_sorted['gas price average']).over(W))\
  .withColumn('var %', f.round((f.col('gas price average')/f.col('gas price average-1') -1)*100,2))
#percent_change.show()
percent_change_only = percent_change.select(col("year"), col("var %"))
display(percent_change_only)
