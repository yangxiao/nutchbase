package org.apache.nutch.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.storage.NutchFields;
import org.apache.nutch.storage.WebTableRow;
import org.apache.nutch.storage.mapreduce.RowMapper;
import org.apache.nutch.storage.mapreduce.RowReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.URLUtil;

public class Generator extends Configured implements Tool {
  public static final String CRAWL_GENERATE_FILTER = "crawl.generate.filter";
  public static final String GENERATE_MAX_PER_HOST = "generate.max.per.host";
  public static final String CRAWL_TOP_N = "crawl.topN";
  public static final String CRAWL_GEN_CUR_TIME = "crawl.gen.curTime";
  public static final String CRAWL_RANDOM_SEED = "generate.partition.seed";
  public static final String CRAWL_ID = "crawl.id";
  
  private static final Set<String> FIELDS = new HashSet<String>();
  
  static {
    FIELDS.add(NutchFields.FETCH_TIME);
    FIELDS.add(NutchFields.SCORE);
    FIELDS.add(NutchFields.STATUS);
  }
  
  public static final Log LOG = LogFactory.getLog(Generator.class);
  public static final byte[] GENERATOR_MARK = null;

  public static class SelectorEntry
  implements WritableComparable<SelectorEntry> {

    String url;
    String host;
    float score;
    
    public SelectorEntry() {  }
    
    public SelectorEntry(String url, float score) {
      this.url = url;
      this.host = URLUtil.getHost(url);
      this.score = score;
    }

    public void readFields(DataInput in) throws IOException {
      url = Text.readString(in);
      host = Text.readString(in);
      score = in.readFloat();
    }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, url);
      Text.writeString(out, host);
      out.writeFloat(score);
    }

    public int compareTo(SelectorEntry se) {
      if (se.score > score)
        return 1;
      else if (se.score == score)
        return url.compareTo(se.url);
      return -1;
    }
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result +  url.hashCode();
      result = prime * result + Float.floatToIntBits(score);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      SelectorEntry other = (SelectorEntry) obj;
      if (!url.equals(other.url))
        return false;
      if (Float.floatToIntBits(score) != Float.floatToIntBits(other.score))
        return false;
      return true;
    }
  }

  public static class SelectorEntryComparator extends WritableComparator {
    public SelectorEntryComparator() {
      super(SelectorEntry.class, true);
    }
  }
  
  static {
    WritableComparator.define(SelectorEntry.class,
                              new SelectorEntryComparator());
  }
  
  /**
   * Mark URLs ready for fetching.
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   * */
  public void generate(long topN, long curTime, boolean filter)
  throws Exception {    
 
    LOG.info("Generator: Selecting best-scoring urls due for fetch.");
    LOG.info("Generator: starting");
    LOG.info("Generator: filtering: " + filter);
    if (topN != Long.MAX_VALUE) {
      LOG.info("Generator: topN: " + topN);
    }
 
    // map to inverted subset due for fetch, sort by score
    getConf().setLong(CRAWL_GEN_CUR_TIME, curTime);
    getConf().setLong(CRAWL_TOP_N, topN);
    getConf().setBoolean(CRAWL_GENERATE_FILTER, filter);
    int randomSeed = new Random().nextInt();
    String crawlId = (curTime / 1000) + "-" + randomSeed;
    getConf().setInt(CRAWL_RANDOM_SEED, randomSeed);
    getConf().set(CRAWL_ID, crawlId);

    Job job = new NutchJob(getConf(), "generate: " + crawlId);
    RowMapper.initRowMapperJob(job, SelectorEntry.class,
        WebTableRow.class, GeneratorMapper.class, FIELDS);
    RowReducer.initRowReducerJob(job, GeneratorReducer.class, PartitionSelectorByHost.class);

    job.waitForCompletion(true);
    
    LOG.info("Generator: done");
    System.out.println("Generated crawl id: " + crawlId);
  }

  public int run(String[] args) throws Exception {
    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    boolean filter = true;

    for (int i = 0; i < args.length; i++) {
      if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[++i]);
      } else if ("-noFilter".equals(args[i])) {
        filter = false;
      }
    }
    
    try {
      generate(topN, curTime, filter);
      return 0;
    } catch (Exception e) {
      LOG.fatal("Generator: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Generator(), args);
    System.exit(res);
  }
}
