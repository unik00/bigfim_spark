/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package be.uantwerpen.adrem.disteclat;

import be.uantwerpen.adrem.hadoop.util.IntArrayWritable;
import be.uantwerpen.adrem.hadoop.util.IntMatrixWritable;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import scala.Int;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

/**
 * Reducer for the first cycle for DistEclat. It receives the complete set of frequent singletons from the different
 * mappers. The frequent singletons are sorted on ascending frequency and distributed among a number of map-tasks.
 * 
 * <pre>
 * {@code
 * Original Input Per Mapper:
 * 
 * 1 2                                      | Mapper 1
 * 1                                        | Mapper 1
 * 
 * 1 2 3                                    | Mapper 2
 * 1 2                                      | Mapper 2
 * 
 * 1 2                                      | Mapper 3
 * 2 3                                      | Mapper 3
 * 
 * 
 * 
 * Example Phase=1, MinSup=1:
 * ==========================
 * 
 * Input:
 * Text                   Iterable<IntArrayWritable>
 * (Prefix)               (<[Mapper ID, Item, Partial TIDs...]>)
 * ""                     <[1,1,0,1],[1,2,0],[2,1,0,1],[2,2,0,1],[2,3,0],[3,1,0],[3,2,0,1],[3,3,1]>
 * 
 * Output:
 * 
 * ==> OSingletonsOrder
 * IntWritable            Writable (Text)
 * (Empty)                (Sorted items)
 *                        1 2 3
 * 
 * 
 * ==> OSingletonsDistribution
 * IntWritable            Writable (Text)
 * (MapperId)             (Assigned singletons)
 * 1                      1
 * 2                      2
 * 3                      3
 * 
 * ==> OSingletonsTids
 * IntWritable            Writable (IntMatrixWritable)
 * (Singleton)            (Partial TIDs)
 * 1                      [[0,1],[0,1],[0]]
 * 2                      [[0],[0,1],[0,1]]
 * 3                      [[],[0],[1]]
 * }
 * </pre>
 */
public class ItemReaderReducer {
  
  public static final Text EmptyKey = new Text("");
  
  private int numberOfMappers;
  private int minSup;
  private final Map<Integer,MutableInt> itemSupports = newHashMap();

  public ItemReaderReducerMultipleOutputs outputs = new ItemReaderReducerMultipleOutputs();
  public List<ItemReaderReducerMultipleOutputs> outputAsList;

  public void setup() {
//    Configuration conf = context.getConfiguration();
//    mos = new MultipleOutputs<>(context);
    /* TODO: write proper config */
//    numberOfMappers = parseInt(conf.get(NUMBER_OF_MAPPERS_KEY, "1"));
//    minSup = conf.getInt(MIN_SUP_KEY, -1);
    minSup = 1;
    numberOfMappers = 10;
//    shortFisFilename = createPath(getJobAbsoluteOutputDir(context), OShortFIs, OShortFIs + "-1");
  }
  
  public void reduce_(Text key, Iterable<int[]> values)
      throws IOException, InterruptedException {

    ArrayList<IntArrayWritable> valuesConverted = new ArrayList<>();
    for(int[] v: values) {
      System.out.println(Arrays.toString(v));
      valuesConverted.add(IntArrayWritable.of(v));
    }
    Map<Integer,IntArrayWritable[]> map = getPartitionTidListsPerExtension(valuesConverted);
    reportItemsWithLargeSupport(map);
  }
  
  private Map<Integer,IntArrayWritable[]> getPartitionTidListsPerExtension(Iterable<IntArrayWritable> values) {
    Map<Integer,IntArrayWritable[]> map = newHashMap();
    for (IntArrayWritable iaw : values) {
      Writable[] w = iaw.get();
      int mapperId = ((IntWritable) w[0]).get();
      int item = ((IntWritable) w[1]).get();
      IntArrayWritable[] tidList = map.get(item);
      if (tidList == null) {
        tidList = new IntArrayWritable[numberOfMappers];
        Arrays.fill(tidList, new IntArrayWritable(new IntWritable[0]));
        map.put(item, tidList);
        itemSupports.put(item, new MutableInt());
      }
      IntWritable[] mapperTids = (IntWritable[]) tidList[mapperId].get();
      IntWritable[] copy = Arrays.copyOf(mapperTids, mapperTids.length + w.length - 2);
      for (int i = w.length - 1, ix = copy.length - 1; i >= 2; i--) {
        copy[ix--] = (IntWritable) w[i];
      }
      tidList[mapperId] = new IntArrayWritable(copy);
      itemSupports.get(item).add(w.length - 2);
    }
    return map;
  }
  
  private void reportItemsWithLargeSupport(Map<Integer,IntArrayWritable[]> map)
      throws IOException, InterruptedException {
    for (Entry<Integer,IntArrayWritable[]> entry : map.entrySet()) {
      int support = 0;
      for (IntArrayWritable iaw : entry.getValue()) {
        support += iaw.get().length;
      }
      if (support < minSup) {
        itemSupports.remove(entry.getKey());
        continue;
      }
      
      final Integer item = entry.getKey();
      final IntArrayWritable[] tids = entry.getValue();
      
      // write the item to the short fis file
//      mos.write(new IntWritable(1), new Text(item + "(" + support + ")"), shortFisFilename);
      outputs.shortFis.add(new Tuple2<>(1, new Text(item + "(" + support + ")").toString()));

      // write the item with the tidlist
//      mos.write(OSingletonsTids, new IntWritable(item), new IntMatrixWritable(tids));
      outputs.OSingletonsTids.add(new Tuple2<>(item, (new IntMatrixWritable(tids)).toIntMatrix() ));
    }
  }
  
  public void cleanup() throws IOException, InterruptedException {
    List<Integer> sortedSingletons = getSortedSingletons();
    
//    context.setStatus("Writing Singletons");
    writeSingletonsOrders(sortedSingletons);
    
//    context.setStatus("Distributing Singletons");
    writeSingletonsDistribution(sortedSingletons);
    
//    context.setStatus("Finished");
//    mos.close();
    outputAsList = Collections.singletonList(outputs);
  }
  
  /**
   * Gets the list of singletons in ascending order of frequency.
   * 
   * @return the sorted list of singletons
   */
  private List<Integer> getSortedSingletons() {
    List<Integer> items = newArrayList(itemSupports.keySet());
    
    Collections.sort(items, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return itemSupports.get(o1).compareTo(itemSupports.get(o2));
      }
    });
    
    return items;
  }
  
  /**
   * Writes the singletons order to the file OSingletonsOrder.
   * 
   * @param sortedSingletons
   *          the sorted singletons
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeSingletonsOrders(List<Integer> sortedSingletons) throws IOException, InterruptedException {
    StringBuilder builder = new StringBuilder();
    for (Integer singleton : sortedSingletons) {
      builder.append(singleton).append(" ");
    }
    
    Text order = new Text(builder.substring(0, builder.length() - 1));
//    mos.write(OSingletonsOrder, EmptyKey, order);
    outputs.OSingletonsOrder.add(new Tuple2<>(-1, order.toString()));
  }
  
  /**
   * Writes the singletons distribution to file OSingletonsDistribution. The distribution is obtained using Round-Robin
   * allocation.
   * 
   * @param sortedSingletons
   *          the sorted list of singletons
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeSingletonsDistribution(List<Integer> sortedSingletons) throws IOException, InterruptedException {
    int end = Math.min(numberOfMappers, sortedSingletons.size());
    
    Text mapperId = new Text();
    Text assignedItems = new Text();
    
    // Round robin assignment
    for (int ix = 0; ix < end; ix++) {
      StringBuilder sb = new StringBuilder();
      for (int ix1 = ix; ix1 < sortedSingletons.size(); ix1 += numberOfMappers) {
        sb.append(sortedSingletons.get(ix1)).append(" ");
      }
      
      mapperId.set("" + ix);
      assignedItems.set(sb.substring(0, sb.length() - 1));
//      mos.write(OSingletonsDistribution, mapperId, assignedItems);
      outputs.OSingletonsDistribution.add(new Tuple2<>(mapperId.toString(), assignedItems.toString()));
    }
  }
}