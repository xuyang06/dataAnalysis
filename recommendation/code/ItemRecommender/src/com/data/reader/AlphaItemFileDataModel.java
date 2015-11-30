package com.data.reader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

import com.google.common.base.Charsets;

public class AlphaItemFileDataModel  {
    private final ItemMemIDMigrator  memIdMigtr = new ItemMemIDMigrator();
    private final ItemMemIDMigrator  userIdMigtr = new ItemMemIDMigrator();
    private final TimeStampMemIDMigrator  timeMemIdMigtr = new TimeStampMemIDMigrator();
    
    private DataModel model;
	private MemoryIDMigrator thing2long = new MemoryIDMigrator();
	private MemoryIDMigrator date2long = new MemoryIDMigrator();
	private String path_str;
    
	public AlphaItemFileDataModel(String path_str){
		this.path_str = path_str;
	}
	
	public void formDataModel() throws TasteException, IOException{
		formDataModel(this.path_str);
	}
	
    public void formDataModel(String dataFilePath) throws TasteException, IOException{
    	Map<Long,List<Preference>> preferecesOfUsers = new HashMap<Long,List<Preference>>();
    	String[] line;
	
    	CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
     
    	//initialize the CSVParser object
    	CSVParser parser = new CSVParser(new FileReader(dataFilePath), format);
    	try{
    		for(CSVRecord record : parser){
    			
				String person = record.get("user_id");
				String itemName = record.get("business_id");
				long rating = Long.parseLong(record.get("stars"));
				String date = record.get("date");
				long userLong = thing2long.toLongID(person);
				thing2long.storeMapping(userLong, person);
				long itemLong = thing2long.toLongID(itemName);
				thing2long.storeMapping(itemLong, itemName);
				
				long dateLong = parseDate(date);
				
				List<Preference> userPrefList;
				
				// if we already have a userPrefList use it
				// otherwise create a new one.
				if((userPrefList = preferecesOfUsers.get(userLong)) == null) {
					userPrefList = new ArrayList<Preference>();
					preferecesOfUsers.put(userLong, userPrefList);
				}
				// add the like that we just found to this user
				userPrefList.add(new GenericPreference(userLong, itemLong, rating));
		    }
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}
    	//close the parser
    	parser.close();
		FastByIDMap<PreferenceArray> preferecesOfUsersFastMap = new FastByIDMap<PreferenceArray>();
		for(Map.Entry<Long, List<Preference>> entry : preferecesOfUsers.entrySet()) {
			preferecesOfUsersFastMap.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue()));
		} 
		this.model = new GenericDataModel(preferecesOfUsersFastMap);
		//this(new AlphaItemFileDataModel(new File("/home/cloudera/pythonWorkspace/dataset-examples-master/dataset/yelp_academic_dataset_review.csv")));
    }
    

	private long parseDate(String stringID) throws ParseException {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		Date d = f.parse(stringID);
	    return d.getTime();
	}
	
	public DataModel getDataModel(){
		return this.model;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
