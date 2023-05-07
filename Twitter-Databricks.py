# Databricks notebook source
# MAGIC %pip install transformers
# MAGIC %pip install scipy
# MAGIC %pip install torch

# COMMAND ----------

from pyspark.sql import SparkSession
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax

# load model and tokenizer
roberta = "cardiffnlp/twitter-roberta-base-sentiment"

model = AutoModelForSequenceClassification.from_pretrained(roberta)
tokenizer = AutoTokenizer.from_pretrained(roberta)

labels = ['Negative', 'Neutral', 'Positive']
databasename = 'testingcosmos'
collection = 'testingdatabrickscosmos'

# COMMAND ----------

spark = SparkSession.builder.appName('testingApp')\
    .config('spark.jars.packages','oorg.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
        .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", databasename).option("collection", collection).load()

# COMMAND ----------

df = df.withColumnRenamed("Created At", "Created_At")
df = df.withColumnRenamed("User ID", "User_ID")

text = df.select("Text")
tweets_list = text.rdd.flatMap(lambda x: x).collect()
result_list = []
for txt in tweets_list:
    encoded_tweet = tokenizer(txt, return_tensors='pt')
    output = model(**encoded_tweet)

    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    ind = scores.argmax()
    result = labels[ind]
    result_list.append(result)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def predict_label(tweet):
    encoded_tweet = tokenizer(tweet, return_tensors='pt')
    output = model(**encoded_tweet)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)
    ind = scores.argmax()
    return labels[ind]

# Register the Python function as a UDF
predict_label_udf = udf(predict_label, StringType())

# Add a new column with the predicted labels using the UDF
result_df = df.withColumn("Sentiment", predict_label_udf(df["Text"]))

# COMMAND ----------

result_df.show()

# COMMAND ----------

result_df.write.saveAsTable('ResultTable')



