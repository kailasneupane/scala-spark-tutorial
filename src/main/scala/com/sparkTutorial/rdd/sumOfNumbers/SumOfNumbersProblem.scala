package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    val conf = new SparkConf().setMaster("local[*]").setAppName("SameHostProblem").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val primes = sc.textFile("in/prime_nums.text")

    val first100primes = primes.flatMap(x => x.split(" "))
      .filter(x => x.matches("[0-9]"))
      .take(100)
      .map(_.toInt)
      .reduce((x, y) => x + y)
    println(first100primes)
  }
}
