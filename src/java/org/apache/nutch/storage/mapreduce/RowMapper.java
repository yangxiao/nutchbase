package org.apache.nutch.storage.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.storage.NutchTableRow;

public class RowMapper<K, R extends NutchTableRow>
extends Mapper<K, R, K, R> {

}
