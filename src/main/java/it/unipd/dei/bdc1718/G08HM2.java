package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

public class G08HM2 {

  //the number of words in text-sample.txt
  private static final long N = 3503570L;

  //class for implement the compare method
  public static class Tuple2Comparator implements Serializable, Comparator<Tuple2<String, Long>> {

      //comparator for mathod top
      public int compare(Tuple2<String, Long> a, Tuple2<String, Long> b) {
          if (a._2() < b._2()) return -1;
          else if (a._2() > b._2()) return 1;
          return 0;
      }
  }

  /*
   * HOMEWORK 2
   * Fate sempre prima l'update del progetto e poi iniziate a scrivere
   * Ho scritto un po' di codice iniziale per ora nulla di strano
   * */

  public static void main(String[] args) throws Exception {
    if (args.length == 0)
      throw new IllegalArgumentException("Expecting the file name on the command line");

    //Variables for measure the time
    long start, end;

    //Creation of the Spark Configuration
    SparkConf configuration = new SparkConf(true );
    configuration.setAppName("Homework 2");

    //Now we can create the Spark Context
    JavaSparkContext sc = new JavaSparkContext(configuration);

    //Creation of the JavaRDD from the text file passed from the command line
    JavaRDD<String> docs = sc.textFile(args[0]).cache().repartition(16);

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

    //Start time 1
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

    //End time 1
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 1: " + (end - start) + " ms");

    //Start time 2
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

    //End time 2
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 2: " + (end - start) + " ms");

    //Start time 3
    start = System.currentTimeMillis();

    //Word Count 3

    //End time 3
    end = System.currentTimeMillis();
    System.out.println("Elapsed time of word count 3: " + (end - start) + " ms");

    //Some print for verifying the correctness of the code
    System.out.println("0. The number of distinct words is " + wordcounts.count());
    System.out.println("1. The number of distinct words is " + wordcounts1.count());
    System.out.println("2. The number of distinct words is " + wordcounts2.count());
    //System.out.println("3. The number of distinct words is " + wordcounts3.count());

    //Part of the k most frequent words
    System.out.println("How much words?");
    Scanner in  = new Scanner(System.in);
    int k = in.nextInt();

    List<Tuple2<String, Long>> k_words = wordcounts.top(k, new Tuple2Comparator());
    for (Tuple2<String, Long> word : k_words) {
        System.out.println("Case 0: " + word._1() + " - " + word._2());
    }

    List<Tuple2<String, Long>> k_words1 = wordcounts.top(k, new Tuple2Comparator());
    for (Tuple2<String, Long> word : k_words1) {
        System.out.println("Case 1: " + word._1() + " - " + word._2());
    }

    List<Tuple2<String, Long>> k_words2 = wordcounts.top(k, new Tuple2Comparator());
    for (Tuple2<String, Long> word : k_words2) {
        System.out.println("Case 2: " + word._1() + " - " + word._2());
    }

    //Stop the end of the program for seeing the web interface
    System.out.println("Press enter to finish");
    System.in.read();
  }
}