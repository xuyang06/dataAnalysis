
package com.data.reader;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;


public class MultithreadedSharingMapper<K1, V1, K2, V2> extends MultithreadedMapper<K1, V1, K2, V2> {

  @Override
  public void run(Context ctx) throws IOException, InterruptedException {
    Class<Mapper<K1, V1, K2, V2>> mapperClass =
        MultithreadedSharingMapper.getMapperClass((JobContext) ctx);
    Preconditions.checkNotNull(mapperClass, "Could not find Multithreaded Mapper class.");

    Configuration conf = ctx.getConfiguration();
    Mapper<K1, V1, K2, V2> mapper1 = ReflectionUtils.newInstance(mapperClass, conf);
    SharingMapper<K1, V1, K2, V2, ?> mapper = null;
    if (mapper1 instanceof SharingMapper) {
      mapper = (SharingMapper<K1, V1, K2, V2, ?>) mapper1;
    }
    Preconditions.checkNotNull(mapper, "Could not instantiate SharingMapper. Class was: %s",
                               mapper1.getClass().getName());

    mapper.setupSharedInstance(ctx);

    super.run(ctx);
  }
}
