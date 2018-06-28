package com.sparkTutorial.pairRdd.filter

import com.sparkTutorial.commons.AirportUtils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsNotInUsaProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text;
       generate a pair RDD with airport name being the key and country name being the value.
       Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located,
       IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       ("Kamloops", "Canada")
       ("Wewak Intl", "Papua New Guinea")
       ...
     */

    val conf = new SparkConf().setMaster("local[*]").setAppName("AirPorts Not in USA")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val path = "in/airports.text"

    val airports = sc.textFile(path).mapPartitions(x => AirportUtils.parseEachLine(x))

    val pair = airports.map(x => (x.name_of_airport, x.country_where_airport_is_located))
      .filter(x => !x._2.equals("United States"))


    pair.coalesce(1).saveAsTextFile("out/airports_not_in_usa_pair_rdd.text")
  }
}
