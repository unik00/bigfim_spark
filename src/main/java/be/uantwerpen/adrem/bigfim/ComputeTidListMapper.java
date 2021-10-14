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
package be.uantwerpen.adrem.bigfim;

import static be.uantwerpen.adrem.bigfim.Tools.convertLineToSet;
import static be.uantwerpen.adrem.bigfim.Tools.getSingletonsFromCountTrie;
import static be.uantwerpen.adrem.bigfim.Tools.readCountTrieFromItemSetsFile;
import static be.uantwerpen.adrem.util.FIMOptions.DELIMITER_KEY;
import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import be.uantwerpen.adrem.util.FIMOptions;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkEnv;

import be.uantwerpen.adrem.hadoop.util.IntArrayWritable;
import be.uantwerpen.adrem.util.ItemSetTrie;
import org.apache.spark.TaskContext;
import scala.Tuple2;

/**
 * Mapper class for the second phase of BigFIM. Each mapper receives a sub part (horizontal cut) of the dataset and
 * computes partial tidlists of the given itemsets in the sub database. The size of the sub database depends on the
 * number of mappers and the size of the original dataset.
 * 
 * <pre>
 * {@code
 *  Original Input Per Mapper:
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
 * LongWritable   Text
 * (Offset)       (Transaction)
 * 0              "1 2"                     | Mapper 1
 * 4              "1"                       | Mapper 1
 * 
 * 6              "1 2 3"                   | Mapper 2
 * 12             "1 2"                     | Mapper 2
 * 
 * 16             "1 2"                     | Mapper 3
 * 20             "2 3"                     | Mapper 3
 * 
 * Output:
 * Text           IntArrayWritable
 * (Prefix)       ([Mapper ID, Item, Partial Tids...])
 * ""             [1,1,0,1]                 | Mapper 1
 * ""             [1,2,0]                   | Mapper 1
 * 
 * ""             [2,1,0,1]                 | Mapper 2
 * ""             [2,2,0,1]                 | Mapper 2
 * ""             [2,3,0]                   | Mapper 2
 * 
 * ""             [3,1,0]                   | Mapper 3
 * ""             [3,2,0,1]                 | Mapper 3
 * ""             [3,3,1]                   | Mapper 3
 * 
 * 
 * 
 * Example Phase=2, MinSup=1:
 * ==========================
 * 
 * Itemsets:
 * 1
 * 2
 * 3
 * 
 * Itemsets of length+1:
 * 1 2
 * 1 3
 * 2 3
 * 
 * Input:
 * LongWritable   Text
 * (Offset)       (Transaction)
 * 0              "1 2"                     | Mapper 1
 * 4              "1"                       | Mapper 1
 * 
 * 6              "1 2 3"                   | Mapper 2
 * 12             "1 2"                     | Mapper 2
 * 
 * 16             "1 2"                     | Mapper 3
 * 20             "2 3"                     | Mapper 3
 * 
 * Output:
 * Text           IntArrayWritable
 * (Prefix)       ([Mapper ID, Item, Partial Tids...])
 * "1"            [1,2,0]                   | Mapper 1
 * 
 * "1"            [2,2,0,1]                 | Mapper 2
 * "1"            [2,3,0]                   | Mapper 2
 * "2"            [2,3,0]                   | Mapper 2
 * 
 * "1"            [3,2,0]                   | Mapper 3
 * "2"            [3,3,1]                   | Mapper 3
 * }
 * </pre>
 */
public class ComputeTidListMapper {
  
  private static int TIDS_BUFFER_SIZE = 100000;
  
  private final IntArrayWritable iaw;

  public List<Tuple2<String, int[]>> result = new ArrayList<>();

  private Set<Integer> singletons;
  private final ItemSetTrie countTrie;
  
  private int phase = 1;
  
  private int counter;
  private int tidCounter = 0;
  private int id;
  
  private String delimiter;
  
  public ComputeTidListMapper() {
    iaw = new IntArrayWritable();
    singletons = null;
    countTrie = new ItemSetTrie.TidListItemsetTrie(-1);
    counter = 0;
    id = -1;
    phase = 1;
  }
  
  public void setup(FIMOptions opt) throws IOException {
//    SparkConf conf = context.getConf();
//    delimiter = conf.get(DELIMITER_KEY, " ");
    delimiter = opt.delimiter;
    // TODO: for BigFIM
/*
    Path[] localCacheFiles = getLocalCacheFiles(conf);
    
    if (localCacheFiles != null) {
      String filename = localCacheFiles[0].toString();
      phase = readCountTrieFromItemSetsFile(filename, countTrie) + 1;
      singletons = getSingletonsFromCountTrie(countTrie);
    }
 */

    id = (int)TaskContext.get().taskAttemptId();
  }
  
//  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//    String line = value.toString();
//    List<Integer> items = convertLineToSet(line, phase == 1, singletons, delimiter);
//    reportItemTids(context, items);
//    counter++;
//  }

  public void map_(List<String> values) throws IOException, InterruptedException {
    for(String line: values) {
      List<Integer> items = convertLineToSet(line, phase == 1, singletons, delimiter);
      reportItemTids(items);
      counter++;
    }
  }

  public void cleanup() throws IOException, InterruptedException {
    if (tidCounter != 0) {
      doRecursiveReport(new StringBuilder(), 0, countTrie);
    }
  }
  
  private IntWritable[] createIntWritableWithIdSet(int numberOfTids) {
    IntWritable[] iw = new IntWritable[numberOfTids + 2];
    iw[0] = new IntWritable(id);
    return iw;
  }
  
  private void reportItemTids(List<Integer> items) throws IOException, InterruptedException {
    if (items.size() < phase) {
      return;
    }
    
    if (phase == 1) {
      for (Integer item : items) {
        countTrie.getChild(item).addTid(counter);
        tidCounter++;
      }
    } else {
      doRecursiveTidAdd(items, 0, countTrie);
    }
    if (tidCounter >= TIDS_BUFFER_SIZE) {
      System.out.println("Tids buffer reached, reporting " + tidCounter + " partial tids");
      doRecursiveReport(new StringBuilder(), 0, countTrie);
      tidCounter = 0;
    }
  }
  
  private void doRecursiveTidAdd(List<Integer> items, int ix, ItemSetTrie trie) {
    for (int i = ix; i < items.size(); i++) {
      ItemSetTrie recTrie = trie.children.get(items.get(i));
      if (recTrie != null) {
        if (recTrie.children.isEmpty()) {
          recTrie.addTid(counter);
          tidCounter++;
        } else {
          doRecursiveTidAdd(items, i + 1, recTrie);
        }
      }
    }
  }
  
  private void doRecursiveReport(StringBuilder builder, int depth, ItemSetTrie trie)
      throws IOException, InterruptedException {
    int length = builder.length();
    for (ItemSetTrie recTrie : trie.children.values()) {
      if (recTrie != null) {
        if (depth + 1 == phase) {
          List<Integer> tids = ((ItemSetTrie.TidListItemsetTrie) recTrie).tids;
          if (tids.isEmpty()) {
            continue;
          }
          Text key = new Text(builder.substring(0, Math.max(0, builder.length() - 1)));
          IntWritable[] iw = createIntWritableWithIdSet(tids.size());
          int i1 = 1;
          iw[i1++] = new IntWritable(recTrie.id);
          for (int tid : tids) {
            iw[i1++] = new IntWritable(tid);
          }
          iaw.set(iw);
//          context.write(key, iaw);
          result.add(new Tuple2<>(key.toString(), iaw.toIntArray()));
          System.out.println("key: " + key + ", iaw: " + iaw);

          tids.clear();
        } else {
          builder.append(recTrie.id).append(" ");
          doRecursiveReport(builder, depth + 1, recTrie);
        }
      }
      builder.setLength(length);
    }
  }
}