package org.apache.nutch.crawl;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.nutch.crawl.Generator.SelectorEntry;
import org.apache.nutch.util.hbase.OldWebTableRow;

public class PartitionSelectorByHost extends Partitioner<SelectorEntry, OldWebTableRow> {

  @Override
  public int getPartition(SelectorEntry key, OldWebTableRow value,
      int numPartitions) {
    int hashCode = key.host.hashCode();

    return (hashCode & Integer.MAX_VALUE) % numPartitions;  
  }
}
