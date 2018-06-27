package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.AirportUtils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */

    val conf = new SparkConf().setMaster("local[*]").setAppName("AirportsByLatitude").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val path = "in/airports.text"

    val airports = sc.textFile(path).mapPartitions(x => AirportUtils.parseEachLine(x))

    val airportsInUSA = airports.filter(x => x.country_where_airport_is_located.equals("United States"))
      .map(x => (x.name_of_airport, x.main_city_served_by_airport))
    airportsInUSA.saveAsTextFile("out/airports_in_usa.text")


  }
}
