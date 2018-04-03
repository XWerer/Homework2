package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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

  public static void main(String[] args) throws Exception {
    if (args.length == 0)
      throw new IllegalArgumentException("Expecting the file name on the command line");

    //Some variables
    long start, end;

    //Creation of the Spark Configuration
    SparkConf configuration = new SparkConf(true );
    configuration.setAppName("Homework 2");

    //Now we can create the Spark Context
    JavaSparkContext sc = new JavaSparkContext(configuration);

    //Creation of the JavaRDD from the text file passed from the command line
    JavaRDD<String> docs = sc.textFile(args[0]).cache();

    //We do the count of the docs for forcing the load in memory
    docs.count();

    //Start time
    start = System.currentTimeMillis();

    //Word Count 1
    JavaPairRDD<String, Long> wordcounts = docs.flatMapToPair((document) -> {
              String[] tokens = document.split(" ");
              ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
              for (String token : tokens) {
                int i = pairs.indexOf(token);
                if (i != -1){
                  Tuple2<String, Long> x = pairs.remove(i);
                  pairs.add(new Tuple2<>(token, x._2 + 1));
                }
                else
                  pairs.add(new Tuple2<>(token, 1L));
              }
              return pairs.iterator();
            }).groupByKey().mapValues((it) -> {
              long sum = 0;
              for (long c : it) {
                sum += c;
              }
              return sum;
            });

    /* For visualizing the results
    wordcounts.foreach(data -> {
      System.out.println(data._1 + " - " + data._2);
    });
    */

    //End time
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 1 " + (end - start) + " ms");

    //Start time
    start = System.currentTimeMillis();

    //Word Count 2

    //End time
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 2 " + (end - start) + " ms");

    //Start time
    start = System.currentTimeMillis();

    //Word Count 3

    //End time
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 3 " + (end - start) + " ms");

    //Stop the end of the program for seeing the web interface
    System.out.println("Press enter to finish");
    System.in.read();
  }
}
