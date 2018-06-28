package com.sparkTutorial.pairRdd.mapValues

import com.sparkTutorial.commons.AirportUtils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsUppercaseProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
       being the key and country name being the value. Then convert the country name to uppercase and
       output the pair RDD to out/airports_uppercase.text

       Each row of the input file contains the following columns:

       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "CANADA")
       ("Wewak Intl", "PAPUA NEW GUINEA")
       ...
     */

    val conf = new SparkConf().setMaster("local[*]").setAppName("AirportsUppercaseProblem").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val path = "in/airports.text"

    val op = sc.textFile(path).mapPartitions(AirportUtils.parseEachLine(_))

    val pair = op.map(x => (x.name_of_airport,x.country_where_airport_is_located.toUpperCase))

    pair.coalesce(1).saveAsTextFile("out/airports_uppercase.text")

  }
}
