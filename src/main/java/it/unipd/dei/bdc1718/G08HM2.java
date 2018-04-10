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
import java.io.*;

/*
 * HOMEWORK 2:
 * Group 08:
 *  Castelletto Riccardo
 *  Masiero Davide
 *  Spadotto Teo
 */

public class G08HM2 {

    //the number of words in text-sample.txt
    private static final long N = 3503570L;

    //class for implement the compare method
    public static class Tuple2Comparator implements Serializable, Comparator<Tuple2<Long, Iterable<String>>> {

        //comparator for method top
        public int compare(Tuple2<Long, Iterable<String>> a, Tuple2<Long, Iterable<String>> b) {
            if (a._1() < b._1()) return -1;
            else if (a._1() > b._1()) return 1;
            return 0;
        }
    }

    //Class for same operation done in the program
    private static class Operation{

        private static long sum(Iterable<Long> it){
            //Reduce phase
            long sum = 0;
            for (long c : it)
                sum += c;
            return sum;
        }

        private static Tuple2<Long, String> swap(Tuple2<String, Long> x){
            return x.swap();
        }
    }

    //Method for printing the most frequency k words
    private static void print(JavaPairRDD<Long, Iterable<String>> swapped, int k){
        List<Tuple2<Long, Iterable<String>>> k_words = swapped.top(k, new Tuple2Comparator());
        for (Tuple2<Long, Iterable<String>> record: k_words){
            System.out.print(record._1() + " ");
            for (String word: record._2())
                System.out.print(word + " ");
            System.out.println();
        }
    }

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


        //***********************TROVA IL NUMERO N***************
        BufferedReader filebuf = new BufferedReader(new FileReader(args[0]));
        String nextStr;
        long parole=0L;
        nextStr = filebuf.readLine();        //legge una intera riga del file
        while (nextStr != null) {
            String[] tokens = nextStr.split(" ");
            parole+= tokens.length;
            nextStr = filebuf.readLine();

        }

        if(parole == N) { System.out.print("PAROLE PAROLE PAROLE "+parole);}


        //***********************TROVA IL NUMERO N**************




        //We do the count of the docs for forcing the load in memory
        System.out.println("The number of documents is " + docs.count());

        //Start time
        start = System.currentTimeMillis();

        //Word count
        JavaPairRDD<String, Long> wordcounts = docs.flatMapToPair((document) -> {
            //Round 1: Map phase
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token : tokens) {
                pairs.add(new Tuple2<>(token, 1L));
            }
            return pairs.iterator();
        }).groupByKey().mapValues(Operation::sum); //Round 1: Reduce Phase

        System.out.println("0. The number of distinct words is " + wordcounts.count());

        //End time
        end = System.currentTimeMillis();
        System.out.println("Elapsed time of word count 0: " + (end - start) + " ms");

        //Start time 1
        start = System.currentTimeMillis();

        //Word Count 1
        JavaPairRDD<String, Long> wordcounts1 = docs.flatMapToPair((document) -> {
            //Round 1: Map phase
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token: tokens) {
                int k = 0;
                for(String tok: tokens){
                    if(token.equals(tok))
                        k++;
                }
                Tuple2<String, Long> x = new Tuple2<>(token, (long) k);
                if(pairs.indexOf(x) == -1)
                    pairs.add(x);
            }
            return pairs.iterator();
        }).groupByKey().mapValues(Operation::sum); //Round 1: Reduce Phase

        System.out.println("1. The number of distinct words is " + wordcounts1.count());

        //End time 1
        end = System.currentTimeMillis();
        System.out.println("Elapsed time of word count 1: " + (end - start) + " ms");

        //Start time 2
        start = System.currentTimeMillis();

        //Word Count 2
        JavaPairRDD<String, Long> wordcounts2 = docs.flatMapToPair((document) -> {
            //Round 1: Map phase
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<Long, Tuple2<String, Long>>> pairs = new ArrayList<>();
            int i = 0; //counter for the key
            for (String token: tokens) {
                int k = 0; //count the number of the same word
                for(String tok: tokens) {
                    if (token.equals(tok))
                        k++;
                }
                boolean find = false; //flag
                //We search if we have already insert the word token
                for (Tuple2<Long, Tuple2<String, Long>> pair: pairs){
                    if (token.equals(pair._2()._1())) {
                        find = true;
                        break;
                    }
                }
                if(!find)       //if we don't have yet inserted the world, we add it using for key a number between 0 and sqrt(N)
                    pairs.add(new Tuple2<>((long) (i++%Math.sqrt(N)), new Tuple2<>(token, (long) k)));
            }
            return pairs.iterator();
        }).groupByKey().flatMapToPair((obj) -> {    //Round 1: Reduce phase
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (Tuple2<String, Long> x : obj._2()){
                boolean find = false; //flag
                //We going to verify if we have already insert a key-value with the same key
                //In case we have already insert it, we update it
                for (int i = 0; i < pairs.size(); i++){
                    if(pairs.get(i)._1().equals(x._1())){
                        pairs.set(i, new Tuple2<>(x._1(), x._2() + pairs.get(i)._2()));
                        find = true;
                        //We can exit from the for because there is only one copy with the same key
                        break;
                    }
                }
                if (!find)
                    //We are sure that we can insert a nuw tuple
                    pairs.add(new Tuple2<>(x._1(), x._2()));
            }
            return pairs.iterator();
        }).groupByKey().mapValues(Operation::sum); //Round 2: Reduce Phase

        System.out.println("2. The number of distinct words is " + wordcounts2.count());

        //End time 2
        end = System.currentTimeMillis();
        System.out.println("Elapsed time of word count 2: " + (end - start) + " ms");

        //Start time 3
        start = System.currentTimeMillis();

        //Word Count 3: The same as word count 2 but using the reduceByKey method
        JavaPairRDD<String, Long> wordcounts3 = docs.flatMapToPair((document) -> {
            //Round 1: Map phase
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token: tokens) {
                int k = 0;
                for(String tok: tokens){
                    if(token.equals(tok))
                        k++;
                }
                Tuple2<String, Long> x = new Tuple2<>(token, (long) k);
                if(pairs.indexOf(x) == -1)
                    pairs.add(x);
            }
            return pairs.iterator();
        }).reduceByKey((x,y) -> x+y);

        System.out.println("3. The number of distinct words is " + wordcounts3.count());

        //End time 3
        end = System.currentTimeMillis();
        System.out.println("Elapsed time of word count 3: " + (end - start) + " ms");

        //Part of the k most frequent words
        System.out.println("How much words?");
        Scanner in  = new Scanner(System.in);
        int k = in.nextInt(); //Number of words
        JavaPairRDD<Long, Iterable<String>> swapped; //For the swapped version of the wordcounts

        //For each wordcounts we are going to swap the key-value and group by key (group the word with the same count), we use the method we have write before
        swapped = wordcounts.mapToPair(Operation::swap).groupByKey();
        print(swapped, k);

        swapped = wordcounts1.mapToPair(Operation::swap).groupByKey();
        print(swapped, k);

        swapped = wordcounts2.mapToPair(Operation::swap).groupByKey();
        print(swapped, k);

        swapped = wordcounts3.mapToPair(Operation::swap).groupByKey();
        print(swapped, k);

        //Stop the end of the program for seeing the web interface
        System.out.println("Press enter to finish");
        System.in.read();
      }
}