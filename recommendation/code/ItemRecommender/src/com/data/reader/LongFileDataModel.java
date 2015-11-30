package com.data.reader;

import java.io.FileReader;
import java.io.IOException;
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
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

public class LongFileDataModel {
	//private final ItemMemIDMigrator  memIdMigtr = new ItemMemIDMigrator();
    //private final ItemMemIDMigrator  userIdMigtr = new ItemMemIDMigrator();
    //private final TimeStampMemIDMigrator  timeMemIdMigtr = new TimeStampMemIDMigrator();
    
    private DataModel model;
	//private MemoryIDMigrator thing2long = new MemoryIDMigrator();
	//private MemoryIDMigrator date2long = new MemoryIDMigrator();
	private String path_str;
	private FastIDSet userIdSet;
	private FastIDSet itemIdSet;
	
	public LongFileDataModel(String path_str){
		this.path_str = path_str;
		this.userIdSet = new FastIDSet(100);
		this.itemIdSet = new FastIDSet(100);
	}
	
	public void formDataModel() throws TasteException, IOException{
		formDataModel(this.path_str);
	}
	
	public boolean containUser(long userId){
		return this.userIdSet.contains(userId);
	}
	
	public boolean containItem(long itemId){
		return this.itemIdSet.contains(itemId);
	}
	
    public void formDataModel(String dataFilePath) throws TasteException, IOException{
    	Map<Long,List<Preference>> preferecesOfUsers = new HashMap<Long,List<Preference>>();
    	String[] line;
	
    	CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
     
    	//initialize the CSVParser object
    	CSVParser parser = new CSVParser(new FileReader(dataFilePath), format);
    	try{
    		for(CSVRecord record : parser){
    			
				long person = Long.parseLong(record.get("user_id"));
				if (!this.userIdSet.contains(person)){
					this.userIdSet.add(person);
				}
				long itemName = Long.parseLong(record.get("business_id"));
				if (!this.itemIdSet.contains(itemName)){
					this.itemIdSet.add(itemName);
				}
				long rating = Long.parseLong(record.get("stars"));
				//long date = Long.parseLong(record.get("date"));
				/*long userLong = thing2long.toLongID(person);
				thing2long.storeMapping(userLong, person);
				long itemLong = thing2long.toLongID(itemName);
				thing2long.storeMapping(itemLong, itemName);
				
				long dateLong = parseDate(date);
				*/
				List<Preference> userPrefList;
				
				// if we already have a userPrefList use it
				// otherwise create a new one.
				if((userPrefList = preferecesOfUsers.get(person)) == null) {
					userPrefList = new ArrayList<Preference>();
					preferecesOfUsers.put(person, userPrefList);
				}
				// add the like that we just found to this user
				userPrefList.add(new GenericPreference(person, itemName, rating));
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
