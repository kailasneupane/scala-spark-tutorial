package com.sparkTutorial.pairRdd.sort

import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem extends App {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */

  val conf = new SparkConf().setMaster("local[*]").setAppName("SortedWordCount")
    .set("spark.hadoop.validateOutputSpecs", "false")

  val sc = new SparkContext(conf)

  val rdd = sc.textFile("in/word_count.text")

  val wc = rdd.flatMap(x => x.split("\\s+"))
    .filter(x => !x.isEmpty)
    .map(x => (x, 1))
    .reduceByKey((x, y) => x + y)
    .sortBy(x => x._2, false)

  wc.coalesce(1).saveAsTextFile("out/SortedWordCountProblem/")

}

