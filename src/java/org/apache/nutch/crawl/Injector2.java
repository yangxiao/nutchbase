package org.apache.nutch.crawl;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.NutchFields;
import org.apache.nutch.storage.NutchHashMap;
import org.apache.nutch.storage.NutchSerializer;
import org.apache.nutch.storage.NutchSerializerFactory;
import org.apache.nutch.storage.WebTableRow;
import org.apache.nutch.storage.mapreduce.NutchTableRowSerialization;
import org.apache.nutch.storage.mapreduce.RowInputFormat;
import org.apache.nutch.storage.mapreduce.RowMapper;
import org.apache.nutch.storage.mapreduce.RowOutputFormat;
import org.apache.nutch.storage.mapreduce.StringSerialization;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.hbase.TableUtil;

public class Injector2
extends RowMapper<String, WebTableRow>
implements Tool {

  public static final Log LOG = LogFactory.getLog(Injector.class);

  private Configuration conf;

  public static class UrlMapper2
  extends Mapper<LongWritable, Text, String, WebTableRow> {
    private NutchSerializer<String, WebTableRow> serializer;

    @Override
    protected void setup(Context context)
    throws IOException, InterruptedException {
      serializer = NutchSerializerFactory.create(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
      String url = value.toString();
      String reversedUrl = TableUtil.reverseUrl(url);
      WebTableRow row = serializer.makeRow();
      row.setMetadata(new NutchHashMap<Utf8, ByteBuffer>(null));
      Mark.INJECT_MARK.putMark(row, TableUtil.YES_VAL);
      context.write(reversedUrl, row);
    }

    @Override
    protected void cleanup(Context context) throws IOException {
      serializer.sync();
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  protected void map(String key, WebTableRow row, Context context)
  throws IOException, InterruptedException {
    if (Mark.INJECT_MARK.checkMark(row) == null) {
      return;
    }
    Mark.INJECT_MARK.removeMark(row);
    if (row.getStatus() == -1) {
      row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
      row.setFetchTime(System.currentTimeMillis());
    }
    context.write(key, row);
  }

  private void inject(Path urlDir) throws Exception {
    LOG.info("Injector: starting");
    LOG.info("Injector: urlDir: " + urlDir);

    getConf().setLong("injector.current.time", System.currentTimeMillis());
    getConf().setStrings("io.serializations", 
        "org.apache.hadoop.io.serializer.WritableSerialization",
        StringSerialization.class.getCanonicalName(),
        NutchTableRowSerialization.class.getCanonicalName());
    Job job = new NutchJob(getConf(), "inject-p1 " + urlDir);
    FileInputFormat.addInputPath(job, urlDir);
    job.setMapperClass(UrlMapper2.class);
    job.setMapOutputKeyClass(String.class);
    job.setMapOutputValueClass(WebTableRow.class);
    job.setOutputFormatClass(RowOutputFormat.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);
    job.waitForCompletion(true);

    getConf().setStrings(RowInputFormat.MAPRED_FIELDS,
        NutchFields.STATUS, NutchFields.METADATA);
    job = new NutchJob(getConf(), "inject-p2 " + urlDir);
    job.setInputFormatClass(RowInputFormat.class);
    job.setMapperClass(Injector2.class);
    job.setMapOutputKeyClass(String.class);
    job.setMapOutputValueClass(WebTableRow.class);
    job.setOutputFormatClass(RowOutputFormat.class);
    job.setReducerClass(Reducer.class);
    job.setNumReduceTasks(0);
    job.waitForCompletion(true);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: Injector <url_dir>");
      return -1;
    }
    try {
      inject(new Path(args[0]));
      return -0;
    } catch (Exception e) {
      LOG.fatal("Injector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(),
        new Injector2(), args);
    System.exit(res);
  }
}
