package com.data.reader;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.TasteHadoopUtils;
import org.apache.mahout.cf.taste.impl.common.FullRunningAverage;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.map.OpenIntObjectHashMap;


public class FactorizationEvaluator extends AbstractJob {

  private static final String USER_FEATURES_PATH = RecommenderJob.class.getName() + ".userFeatures";
  private static final String ITEM_FEATURES_PATH = RecommenderJob.class.getName() + ".itemFeatures";

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new FactorizationEvaluator(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    addInputOption();
    addOption("userFeatures", null, "path to the user feature matrix", true);
    addOption("itemFeatures", null, "path to the item feature matrix", true);
    addOption("usesLongIDs", null, "input contains long IDs that need to be translated", String.valueOf(false));
    addOutputOption();
    String[] argsCopy = new String[args.length-1];
    for (int i = 1; i < args.length; i ++){
    	argsCopy[i-1] = args[i];
    }
    Map<String,List<String>> parsedArgs = parseArguments(argsCopy);
    if (parsedArgs == null) {
      return -1;
    }

    Path errors = getTempPath("errors");

    Job predictRatings = prepareJob(getInputPath(), errors, TextInputFormat.class, PredictRatingsMapper.class,
        DoubleWritable.class, NullWritable.class, SequenceFileOutputFormat.class);

    Configuration conf = predictRatings.getConfiguration();
    conf.set(USER_FEATURES_PATH, getOption("userFeatures"));
    conf.set(ITEM_FEATURES_PATH, getOption("itemFeatures"));

    boolean usesLongIDs = Boolean.parseBoolean(getOption("usesLongIDs"));
    if (usesLongIDs) {
      conf.set(ALSRecommender.USES_LONG_IDS, String.valueOf(true));
    }


    boolean succeeded = predictRatings.waitForCompletion(true);
    if (!succeeded) {
      return -1;
    }

    BufferedWriter writer  = null;
    try {
      FileSystem fs = FileSystem.get(getOutputPath().toUri(), getConf());
      FSDataOutputStream outputStream = fs.create(getOutputPath("rmse.txt"));
      double rmse = computeRmse(errors);
      writer = new BufferedWriter(new OutputStreamWriter(outputStream, Charsets.UTF_8));
      writer.write(String.valueOf(rmse));
    } finally {
      Closeables.close(writer, false);
    }

    return 0;
  }

  double computeRmse(Path errors) {
    RunningAverage average = new FullRunningAverage();
    for (Pair<DoubleWritable,NullWritable> entry
        : new SequenceFileDirIterable<DoubleWritable, NullWritable>(errors, PathType.LIST, PathFilters.logsCRCFilter(),
          getConf())) {
      DoubleWritable error = entry.getFirst();
      average.addDatum(error.get() * error.get());
    }

    return Math.sqrt(average.getAverage());
  }

  public static class PredictRatingsMapper extends Mapper<LongWritable,Text,DoubleWritable,NullWritable> {

    private OpenIntObjectHashMap<Vector> U;
    private OpenIntObjectHashMap<Vector> M;

    private boolean usesLongIDs;

    private final DoubleWritable error = new DoubleWritable();

    @Override
    protected void setup(Context ctx) throws IOException, InterruptedException {
      Configuration conf = ctx.getConfiguration();

      Path pathToU = new Path(conf.get(USER_FEATURES_PATH));
      Path pathToM = new Path(conf.get(ITEM_FEATURES_PATH));

      U = ALS.readMatrixByRows(pathToU, conf);
      M = ALS.readMatrixByRows(pathToM, conf);

      usesLongIDs = conf.getBoolean(ALSRecommender.USES_LONG_IDS, false);
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {

      String[] tokens = TasteHadoopUtils.splitPrefTokens(value.toString());

      int userID = TasteHadoopUtils.readID(tokens[TasteHadoopUtils.USER_ID_POS], usesLongIDs);
      int itemID = TasteHadoopUtils.readID(tokens[TasteHadoopUtils.ITEM_ID_POS], usesLongIDs);
      double rating = Double.parseDouble(tokens[2]);

      if (U.containsKey(userID) && M.containsKey(itemID)) {
        double estimate = U.get(userID).dot(M.get(itemID));
        error.set(rating - estimate);
        ctx.write(error, NullWritable.get());
      }
    }
  }

}
