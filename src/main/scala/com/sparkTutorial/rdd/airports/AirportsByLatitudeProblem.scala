package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.AirportUtils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    val conf = new SparkConf().setMaster("local[*]").setAppName("AirportsByLatitude").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val path = "in/airports.text"

    val airports = sc.textFile(path).mapPartitions(x => AirportUtils.parseEachLine(x))

    val airportsWithMoreThan40Latitudes = airports.filter(x => x.latitude.toDouble > 40.0)

    val output = airportsWithMoreThan40Latitudes.map(x => (x.name_of_airport, x.latitude))

    output.coalesce(1).saveAsTextFile("out/airports_by_latitude.text")

  }
}
