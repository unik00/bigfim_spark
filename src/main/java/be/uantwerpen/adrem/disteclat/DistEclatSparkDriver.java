package be.uantwerpen.adrem.disteclat;

import be.uantwerpen.adrem.bigfim.ComputeTidListMapper;
import be.uantwerpen.adrem.util.FIMOptions;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static be.uantwerpen.adrem.hadoop.util.SplitByKTextInputFormat.NUMBER_OF_CHUNKS;
import static be.uantwerpen.adrem.util.FIMOptions.*;

public class DistEclatSparkDriver {
    public static void main(String[] args){
        FIMOptions opt = new FIMOptions();
        if (!opt.parseOptions(args)) {
            opt.printHelp();
        }

        SparkConf sparkConf = new SparkConf().setAppName("DistEclatSpark");
//        .set("spark.driver.maxResultSize", "40g").set("spark.memory.storageFraction", "0.1").set("spark.driver.cores", "4");

        sparkConf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> transactions = sc.textFile(opt.inputFile, 1).cache();

        JavaRDD<List<String>> transactionsPart = transactions.glom();
        JavaPairRDD<String, int[]> computeTidListMapperResult = JavaPairRDD.fromJavaRDD(transactionsPart.mapPartitions(
                iter -> {
                    ComputeTidListMapper mapper = new ComputeTidListMapper();
                    mapper.setup(opt);
                    while (iter.hasNext()){
                        mapper.map_(iter.next());
                    }
                    mapper.cleanup();

                    return mapper.result.iterator();
                }
            )
        );
        for(Tuple2<String, Iterable<int[]>> clgt: computeTidListMapperResult.groupByKey().collect()){
            assert(clgt._2 != null);
            Iterable<int[]> values = clgt._2;
            for(int[] v: values){
                System.out.println("wtf");
                System.out.println(Arrays.toString(v));
            }
        }

        JavaRDD<ItemReaderReducerMultipleOutputs> itemReaderReducerResult = computeTidListMapperResult.
                groupByKey().mapPartitions(
                iter -> {
                    ItemReaderReducer reducer = new ItemReaderReducer();
                    reducer.setup(opt);
                    while (iter.hasNext()){
                        Tuple2<String, Iterable<int[]>> pair = iter.next();
                        Text key = new Text(pair._1);
                        Iterable<int[]> values = pair._2;
                        reducer.reduce_(key, values);
                    }
                    reducer.cleanup();
                    return reducer.outputAsList.iterator();
                }
        );
        for(ItemReaderReducerMultipleOutputs result: itemReaderReducerResult.collect()){
            System.out.println(result.toString());
        }

    }
}
