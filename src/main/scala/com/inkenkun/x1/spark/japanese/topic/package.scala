package com.inkenkun.x1.spark.japanese

import scala.collection.JavaConverters._

import com.typesafe.config.Config
import org.apache.spark.SparkConf

package object topic {
  
  implicit class convertSparkConf( config: Config ) {
    def toSparkConf: SparkConf =
      config.entrySet.asScala.foldLeft( new SparkConf() ) { (spark, entry) =>
        spark.set( s"spark.${entry.getKey}", config.getString(entry.getKey) )
      }
  }
}
