package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class G08HM2 {

  /*
   * HOMEWORK 2
   * Fate sempre prima l'update del progetto e poi iniziate a scrivere
   * Ho scritto un po' di codice iniziale per ora nulla di strano 
   * */

  public static void main(String[] args) throws IllegalArgumentException {
    if (args.length == 0)
      throw new IllegalArgumentException("Expecting the file name on the command line");

    //Creation of the Spark Configuration
    SparkConf configuration = new SparkConf(true );
    configuration.setAppName("Homework 2");
    configuration.setMaster("<master>");

    //Now we can create the Spark Context
    JavaSparkContext sc = new JavaSparkContext(configuration);

    //Creation of the JavaRDD from the text file passed from the command line
    JavaRDD<String> docs = sc.textFile(args[0]).cache();

    //We do the count of the docs for forcing to load in memory
    docs.count();

    //Start time
    long start = System.currentTimeMillis();

    //Code to write

    //End time
    long end = System.currentTimeMillis();
    System.out.println("Elapsed time " + (end - start) + " ms");

  }
}
