package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import com.sparkTutorial.commons.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the house data from in/RealEstate.csv,
       output the average price for houses with different number of bedrooms.

    The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
    around it. 

    The dataset contains the following fields:
    1. MLS: Multiple listing service number for the house (unique ID).
    2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
    northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
    some out of area locations as well.
    3. Price: the most recent listing price of the house (in dollars).
    4. Bedrooms: number of bedrooms.
    5. Bathrooms: number of bathrooms.
    6. Size: size of the house in square feet.
    7. Price/SQ.ft: price of the house per square foot.
    8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

    Each field is comma separated.

    Sample output:

       (3, 325000)
       (1, 266356)
       (2, 325000)
       ...

       3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
     */

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("AverageHouseProblem")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val path = "in/RealEstate.csv"

    val AvgHouse = sc.textFile(path)
    var header = AvgHouse.first()

    //bedroom, price
    val AvgHouseFiltered = AvgHouse.filter(x => !x.equals(header))
      .map(x => (x.split(Utils.COMMA_DELIMITER)(3), x.split(Utils.COMMA_DELIMITER)(2)))

    val grouped = AvgHouseFiltered.groupBy(x => x._1)
     .map(x => (x._1,x._2.map(y => y._2.toFloat)))
      .map(x => (
       x._1.toInt, x._2.reduce((a,b) => a + b) / x._2.size))

    grouped.coalesce(1).saveAsTextFile("out/houseProblem/")

  }

}
