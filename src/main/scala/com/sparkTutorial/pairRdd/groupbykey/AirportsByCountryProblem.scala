package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.AirportUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountryProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ...
     */

    val conf = new SparkConf().setMaster("local[*]").setAppName("AirportsByCountryProblem").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val path = "in/airports.text"

    val op = sc.textFile(path).mapPartitions(AirportUtils.parseEachLine(_))

    val pair: RDD[(String, Iterable[String])] = op.map(x => (x.country_where_airport_is_located, x.name_of_airport))
      .groupByKey().map(x => (x._1, x._2.toList))

    pair.coalesce(1).saveAsTextFile("out/AirportsByCountryProblem/")

  }
}
