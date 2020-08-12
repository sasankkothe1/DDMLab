package com.fastajava.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;

public class App
{
    public static void main( String[] args ) throws IOException {
	SparkConf conf = new SparkConf().setAppName(args[0] + "mers Program");
        Configuration confFS = new Configuration();
        JavaSparkContext context = new JavaSparkContext(conf);
        confFS.addResource("/home/hadoop/hadoop/etc/hadoop/conf/core-site.xml");
        confFS.addResource("/home/hadoop/hadoop/etc/hadoop/conf/hdfs-site.xml");
        confFS.set("fs.default.name", "hdfs:/"+"/master:9000");
        int k = Integer.parseInt(args[0]);

        final Broadcast<Integer> bk = context.broadcast(k);

        //read fasta_file
        JavaRDD<String> records = context.textFile("hdfs://master:9000/data/input/uniref90_infl.fasta", 1);
        JavaRDD<String> filteredRDD = records.filter((Function<String, Boolean>) record -> {
            if(record.length() > 0){
                String firstChar = record.substring(0,1);
                if ( firstChar.equals("#") || firstChar.equals(">")) {
                    return false; // do not return these records
                }
                else {
                    return true;
                }

            }
            else{ return false;}


        });

        JavaPairRDD<String,Integer> kmers = filteredRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer> () {
           public Iterator<Tuple2<String,Integer>> call(String sequence) {
	    int k1 = bk.value();
            List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
            for (int i = 0; i < sequence.length()- k1 +1 ; i++) {
                String kmer = sequence.substring(i, k1 +i);
                list.add(new Tuple2<String,Integer>(kmer, 1));
            }
            return list.iterator();
	}
        });

        filteredRDD = null;
        JavaPairRDD<String, Integer> kmersGrouped = kmers.reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);
        kmers = null;

        JavaPairRDD<String, Integer> sortedPairRDD = kmersGrouped.sortByKey();


        sortedPairRDD.saveAsTextFile("/data/output/"+k+"mer.txt");

        // done
        context.close();
        System.exit(0);

    }
}

