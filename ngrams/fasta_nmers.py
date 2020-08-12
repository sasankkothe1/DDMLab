#!/usr/bin/python

from pyspark.ml.feature import NGram
from pyspark.sql import SparkSession
from read_fasta import read_fasta

if __name__ == "__main__":
        spark = SparkSession.builder.appName("4gram").getOrCreate()

        fasta_line = list(read_fasta("tests.fasta"))
        df = spark.createDataFrame([(0, fasta_line)], ["id", "aminos"])
        ngram = NGram(n=4, inputCol="aminos", outputCol="ngrams_aminos")
        ngram_df = ngram.transform(df)
        ngram_df.select("ngrams_aminos").show(truncate=False)
        ngram_list = ngram_df.select("ngrams_aminos").collect()[0][0]
        ngrams = {}
        for ngram in ngram_list:
                if ngram in ngrams:
                        ngrams[ngram] += 1
                else:
                        ngrams[ngram] = 1
        print ngrams
        
        spark.stop()
