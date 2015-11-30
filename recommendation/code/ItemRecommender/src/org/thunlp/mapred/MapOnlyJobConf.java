package org.thunlp.mapred;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class MapOnlyJobConf extends MapReduceJobConf {
    public MapOnlyJobConf () {
        super();
        this.setReducerClass(IdentityReducer.class);
        this.setNumReduceTasks(0);
    }
    
    public MapOnlyJobConf ( Class jobClass ) {
        super(jobClass);
        this.setReducerClass(IdentityReducer.class);
        this.setNumReduceTasks(0);
    }
    
    public void setKeyValueClass(Class<? extends WritableComparable> keyMerge, 
    		Class<? extends Writable> valueMerge ) {
        this.setMapOutputKeyClass(keyMerge);
        this.setMapOutputValueClass(valueMerge);
        this.setOutputKeyClass(keyMerge);
        this.setOutputValueClass(valueMerge);
    }
}
