package be.uantwerpen.adrem.disteclat;

import be.uantwerpen.adrem.bigfim.ComputeTidListMapper;
import be.uantwerpen.adrem.hadoop.util.IntArrayWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

public class DistEclatSparkDriver {
    public static void main(String[] args){
        String input = args[0];
        long minSup = Long.parseLong(args[1]);
        long G = Long.parseLong(args[2]);

        SparkConf sparkConf = new SparkConf().setAppName("BIGMiner").set("spark.driver.maxResultSize", "40g").set("spark.memory.storageFraction", "0.1").set("spark.driver.cores", "4");

        sparkConf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> transactions = sc.textFile(input, 3).cache();
        JavaRDD<List<String>> transactionsPart = transactions.glom();
        JavaRDD<Tuple2<Text, IntArrayWritable> > computeTidListMapperResult = transactionsPart.mapPartitions(
                iter -> {
                    ComputeTidListMapper mapper = new ComputeTidListMapper();
                    mapper.setup(sc);
                    if (!iter.hasNext()){
                        mapper.cleanup();
                    }
                    return mapper.result.iterator();
                }
        );



    }
}