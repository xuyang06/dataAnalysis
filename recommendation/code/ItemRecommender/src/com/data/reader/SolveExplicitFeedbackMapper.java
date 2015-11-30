
package com.data.reader;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import org.apache.mahout.math.map.OpenIntObjectHashMap;

import java.io.IOException;

/** Solving mapper that can be safely executed using multiple threads */
public class SolveExplicitFeedbackMapper
    extends SharingMapper<IntWritable,VectorWritable,IntWritable,VectorWritable,OpenIntObjectHashMap<Vector>> {

  private double lambda;
  private int numFeatures;
  private final VectorWritable uiOrmj = new VectorWritable();

  @Override
  OpenIntObjectHashMap<Vector> createSharedInstance(Context ctx) throws IOException {
    Configuration conf = ctx.getConfiguration();
    int numEntities = Integer.parseInt(conf.get(ALSRecommender.NUM_ENTITIES));
    return ALS.readMatrixByRowsFromDistributedCache(numEntities, conf);
  }

  @Override
  protected void setup(Mapper.Context ctx) throws IOException, InterruptedException {
    lambda = Double.parseDouble(ctx.getConfiguration().get(ALSRecommender.LAMBDA));
    numFeatures = ctx.getConfiguration().getInt(ALSRecommender.NUM_FEATURES, -1);
    Preconditions.checkArgument(numFeatures > 0, "numFeatures must be greater then 0!");
  }

  @Override
  protected void map(IntWritable userOrItemID, VectorWritable ratingsWritable, Context ctx)
    throws IOException, InterruptedException {
    OpenIntObjectHashMap<Vector> uOrM = getSharedInstance();
    uiOrmj.set(ALS.solveExplicit(ratingsWritable, uOrM, lambda, numFeatures));
    ctx.write(userOrItemID, uiOrmj);
  }

}
