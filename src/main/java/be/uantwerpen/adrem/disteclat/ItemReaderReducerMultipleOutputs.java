package be.uantwerpen.adrem.disteclat;

import org.apache.hadoop.io.Writable;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ItemReaderReducerMultipleOutputs implements Serializable {
    public List<Tuple2<Integer, String>> OSingletonsOrder;
    public List<Tuple2<String, String>> OSingletonsDistribution;
    public List<Tuple2<Integer, int[][]>> OSingletonsTids;
    public List<Tuple2<Integer, String>> shortFis;

    public ItemReaderReducerMultipleOutputs(){
        OSingletonsDistribution = new ArrayList<>();
        OSingletonsTids = new ArrayList<>();
        shortFis = new ArrayList<>();
        OSingletonsOrder = new ArrayList<>();
    }

    @Override
    public String toString() {
        String s = "ItemReaderReducerMultipleOutputs{" +
                "OSingletonsOrder=" + OSingletonsOrder +
                ", OSingletonsDistribution=" + OSingletonsDistribution +
                ", shortFis=" + shortFis +
                ", OSingletonsTids=[";
        for(Tuple2<Integer, int[][]> tp : OSingletonsTids){
            s += "(";
            s += tp._1.toString() + ", ";
            s += Arrays.deepToString(tp._2);
            s += ")";
        }
        s += "]}";
        return s;
    }
}
