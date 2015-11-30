package com.data.reader;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.Recommender;

public class FileEstimator {
	
	private DataModel model;
	//private MemoryIDMigrator thing2long = new MemoryIDMigrator();
	//private MemoryIDMigrator date2long = new MemoryIDMigrator();
	private String path_str;
	private LongFileDataModel fileDataModel;
	private Recommender recommender;
	//private FastIDSet userIdSet;
	//private FastIDSet itemIdSet;
	
	public FileEstimator(String path_str, LongFileDataModel fileDataModel, Recommender recommender){
		this.path_str = path_str;
		this.fileDataModel = fileDataModel;
		this.recommender = recommender;
		//this.itemIdSet = new FastIDSet(100);
	}
	
	
	public double calMSE() throws TasteException, IOException{
    	//Map<Long,List<Preference>> preferecesOfUsers = new HashMap<Long,List<Preference>>();
    	//String[] line;
	
    	CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
     
    	//initialize the CSVParser object
    	CSVParser parser = new CSVParser(new FileReader(this.path_str), format);
    	int total_num = 0;
		long mse = 0;
    	try{
    		for(CSVRecord record : parser){
    			
				long person = Long.parseLong(record.get("user_id"));
				/*if (!this.userIdSet.contains(person)){
					this.userIdSet.add(person);
				}*/
				long itemName = Long.parseLong(record.get("business_id"));
				/*if (!this.itemIdSet.contains(itemName)){
					this.itemIdSet.add(itemName);
				}*/
				long rating = Long.parseLong(record.get("stars"));
				
				if (this.fileDataModel.containUser(person) && this.fileDataModel.containItem(itemName)){
					long estimate_rating = (long) this.recommender.estimatePreference(person, itemName);
					mse = mse + (estimate_rating - rating)*(estimate_rating - rating);
					total_num += 1;
				}
		    }
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}
    	//close the parser
    	parser.close();
    	double avg_mse = Math.sqrt((double)mse/total_num);
		return avg_mse;
		//this(new AlphaItemFileDataModel(new File("/home/cloudera/pythonWorkspace/dataset-examples-master/dataset/yelp_academic_dataset_review.csv")));
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
