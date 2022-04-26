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
data = json.dumps({"seriesid": ['APU000074712','CUSR0000SA0'],"startyear":"1986", "endyear":"2014"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
#print(p.text)
json_data = json.loads(p.text)
print(json_data)
#dbutils.fs.put("dbfs:/FileStore/my-stuff/gasoline-file.json", json_data)
#json_data.write.format("json").save("dbfs:/FileStore/my-stuff/gasoline-file.json")

print(os.getcwd())
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
    #dbutils.fs.put('/databricks/driver/' + seriesId  + '.txt', json_data)
    output = open(seriesId + '.txt','w')
    #print(seriesId)
    output.write (x.get_string())
    dbutils.fs.put("dbfs:/FileStore/my-stuff/" + seriesId + '.txt', x.get_string())
    #print(x)
    output.close()

# COMMAND ----------

#%sh ls /databricks/driver
display(dbutils.fs.ls("dbfs:/FileStore/my-stuff"))

# COMMAND ----------

invoices = sqlContext.read.text("dbfs:/FileStore/my-stuff/APU000074712.txt").rdd
display(invoices.take(5))
#df = spark.read.text(
#  'dbfs:/FileStore/my-stuff/APU000074712.txt'
#)
#df.printSchema()
#display(df.take(10))

# COMMAND ----------

#%sh ls -la
#%sh pwd
#dbutils.fs.rm("/FileStore/my-stuff/gasoline-file.txt")
#dbutils.fs.cp("/databricks/driver/APU000074712.txt", "/FileStore/my-stuff/gasoline-file.txt")

# COMMAND ----------

#aws_bucket_name = "db-b36f5d53bd16a4142b5df0c0e016e20b-s3-root-bucket"
#mount_name = "my_mount"
#dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
#display(dbutils.fs.ls("/mnt/%s" % mount_name))
