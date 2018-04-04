package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;
import scala.Tuple3;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class G08HM2 {

  //the number of words in text-sample.txt
  private static final long N = 3503570L;

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
    JavaRDD<String> docs = sc.textFile(args[0]).cache().repartition(8);

    //We do the count of the docs for forcing the load in memory
    System.out.println("The number of documents is " + docs.count());

    //Start time
    start = System.currentTimeMillis();

    // Word count
    JavaPairRDD<String, Long> wordcounts = docs.flatMapToPair((document) -> {
              String[] tokens = document.split(" ");
              ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
              for (String token : tokens) {
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

    //End time
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 0: " + (end - start) + " ms");

    //Start time
    start = System.currentTimeMillis();

    //Word Count 1
    JavaPairRDD<String, Long> wordcounts1 = docs.flatMapToPair((document) -> {
              String[] tokens = document.split(" ");
              ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
              for (String token : tokens) {
                int i = pairs.indexOf(token);
                if (i != -1){
                  Tuple2<String, Long> x = pairs.remove(i);
                  pairs.add(new Tuple2<>(token, x._2() + 1));
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
    wordcount1.foreach(data -> {
      System.out.println(data._1 + " - " + data._2);
    });
    */

    //End time
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 1: " + (end - start) + " ms");

    //Start time
    start = System.currentTimeMillis();

    //Word Count 2
    JavaPairRDD<String, Long> wordcounts2 = docs.flatMapToPair((document) -> {
                String[] tokens = document.split(" ");
                ArrayList<Tuple2<Long, Tuple2<String, Long>>> pairs = new ArrayList<>();
                for (int i = 0; i < tokens.length; i++){
                    int j = pairs.indexOf(tokens[i]);
                    if (j != -1){
                      Tuple2<Long, Tuple2<String, Long>> x = pairs.remove(j);
                      pairs.add(new Tuple2<>(x._1(), new Tuple2<>(tokens[i], x._2()._2() + 1L)));
                    }
                    else
                      pairs.add(new Tuple2<>( (long) (i%Math.sqrt(N)), new Tuple2<>(tokens[i], 1L)));
                }
                return pairs.iterator();
            }).groupByKey().flatMapToPair((pair) -> {
                ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                for (Tuple2<String, Long> x : pair._2()) {
                    int j = pairs.indexOf(x._1());
                    if(j != -1){
                        Tuple2<String, Long> y = pairs.remove(j);
                        pairs.add(new Tuple2<>(x._1(), x._2() + y._2()));
                    }
                    else
                        pairs.add(new Tuple2<>(x._1(), x._2()));
                }
                return pairs.iterator();
            }).reduceByKey((x,y) -> x+y);

    //End time
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 2: " + (end - start) + " ms");

    //Start time
    start = System.currentTimeMillis();

    //Word Count 3
    //End time
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 3: " + (end - start) + " ms");

    System.out.println("0. The number of distinct words is " + wordcounts.count());
    System.out.println("1. The number of distinct words is " + wordcounts1.count());
    System.out.println("2. The number of distinct words is " + wordcounts2.count());
    //System.out.println("3. The number of distinct words is " + wordcounts.count());
    System.out.println("      Insert the number you want   ");
    Scanner scanner = new Scanner(System.in);
    int k = scanner.nextInt();

      JavaPairRDD<String, Long> last = wordcounts2.mapToPair(x -> x.swap())
            .sortByKey(false)
            .mapToPair(x -> x.swap());

        /*
      last.foreach(data -> {
          for(int i=0; i<k;i++)
          System.out.println("word : "+data._1() + " number occurences=" + data._2());
      });

        */


      //Stop the end of the program for seeing the web interface
    System.out.println("Press enter to finish");
    System.in.read();
  }
}