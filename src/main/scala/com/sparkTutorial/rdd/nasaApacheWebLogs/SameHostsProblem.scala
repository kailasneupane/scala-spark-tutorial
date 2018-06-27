package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    val conf = new SparkConf().setMaster("local[*]").setAppName("SameHostProblem").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val path1 = "in/nasa_19950701.tsv"
    val path2 = "in/nasa_19950801.tsv"

    val nasaJuly1st:RDD[String] = sc.textFile(path1).map(x => x.split("\t")(0))
    val nasaAugust1st:RDD[String] = sc.textFile(path2).map(x => x.split("\t")(0))

    val both = nasaJuly1st.intersection(nasaAugust1st).filter(x => !x.equals("host"))
    both.foreach(println)
    both.saveAsTextFile("out/nasa_logs_same_hosts.csv")

  }
}
