/**
  * Created by tariq on 04/02/2017.
  */

package com.ezzibdeh.hadoop


import org.apache.spark.{SparkConf,SparkContext}

object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("New test")
      .setMaster("local")
      .set("spark.executor.memory","2g")

    val sc = new SparkContext(conf)


    val lines = sc.parallelize(Seq("This is the first line", " this is the Second line", " this is the third!"))
    //define the parallelize actions in context

    val counts = lines.flatMap(line => line.split(" "))
      .map(word=> (word,1))
      .reduceByKey(_+_)
    counts.foreach(println) //count word and display

  }
}
