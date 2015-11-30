package com.data.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ItemBasedRecommender implements Recommender {

	  private Recommender delegate;
	  //private DataModel model;
	  //private MemoryIDMigrator thing2long = new MemoryIDMigrator();
	  //private MemoryIDMigrator date2long = new MemoryIDMigrator();

	  /*public ItemBasedRecommender() throws TasteException, IOException {
		  
			// create a map for saving the preferences (likes) for
			// a certain person
			Map<Long,List<Preference>> preferecesOfUsers = new HashMap<Long,List<Preference>>();
			
			
			
			String[] line;
			
			CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
	         
	        //initialize the CSVParser object
	        CSVParser parser = new CSVParser(new FileReader("employees.csv"), format);
	         
	        //List<Employee> emps = new ArrayList<Employee>();
	        try{
	        for(CSVRecord record : parser){
	        	
	        	//String person = line[0];
				//String itemName = line[1];
				
				String person = record.get("user_id");
				String itemName = record.get("business_id");
				long rating = Long.parseLong(record.get("stars"));
				String date = record.get("date");
				
				// other lines contained but not used
				// String category = line[2];
				// String id = line[3];
				// String created_time = line[4];
				
				// create a long from the person name
				long userLong = thing2long.toLongID(person);
				
				// store the mapping for the user
				thing2long.storeMapping(userLong, person);
				
				// create a long from the like name
				long itemLong = thing2long.toLongID(itemName);
				
				// store the mapping for the item
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
				userPrefList.add(new GenericPreference(userLong, itemLong, 1));
	        	
	        	Employee emp = new Employee();
	            emp.setId(record.get("ID"));
	            emp.setName(record.get("Name"));
	            emp.setRole(record.get("Role"));
	            emp.setSalary(record.get("Salary"));
	            emps.add(emp);
	        	}
	        }catch(Exception ex){
	        	ex.printStackTrace();
	        }
	        //close the parser
	        parser.close();
			
			// go through every line
			while((line = parser.getLine()) != null) {
			
				String person = line[0];
				String likeName = line[1];
				
				// other lines contained but not used
				// String category = line[2];
				// String id = line[3];
				// String created_time = line[4];
				
				// create a long from the person name
				long userLong = thing2long.toLongID(person);
				
				// store the mapping for the user
				thing2long.storeMapping(userLong, person);
				
				// create a long from the like name
				long itemLong = thing2long.toLongID(likeName);
				
				// store the mapping for the item
				thing2long.storeMapping(itemLong, likeName);
				
				List<Preference> userPrefList;
				
				// if we already have a userPrefList use it
				// otherwise create a new one.
				if((userPrefList = preferecesOfUsers.get(userLong)) == null) {
					userPrefList = new ArrayList<Preference>();
					preferecesOfUsers.put(userLong, userPrefList);
				}
				// add the like that we just found to this user
				userPrefList.add(new GenericPreference(userLong, itemLong, 1));
				log.fine("Adding "+person+"("+userLong+") to "+likeName+"("+itemLong+")");
			}
			
			// create the corresponding mahout data structure from the map
			FastByIDMap<PreferenceArray> preferecesOfUsersFastMap = new FastByIDMap<PreferenceArray>();
			for(Map.Entry<Long, List<Preference>> entry : preferecesOfUsers.entrySet()) {
				preferecesOfUsersFastMap.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue()));
			}

			// create a data model 
			DataModel dataModel = new GenericDataModel(preferecesOfUsersFastMap);
			initializeItemBasedRecommender(dataModel);
	    //this(new AlphaItemFileDataModel(new File("/home/cloudera/pythonWorkspace/dataset-examples-master/dataset/yelp_academic_dataset_review.csv")));
	  }*/
	  
	  /*public long parseDate(String stringID) throws ParseException {
	    	SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
	    	Date d = f.parse(stringID);
	        return d.getTime();
	  }*/
	  
	  public ItemBasedRecommender(DataModel model)
	      throws TasteException, IOException {
	    UserSimilarity similarity = new EuclideanDistanceSimilarity(model);
	    UserNeighborhood neighborhood =
	        new NearestNUserNeighborhood(2, similarity, model);
	    delegate =
	        new GenericUserBasedRecommender(model, neighborhood, similarity);
	    //this.model = model;
	    
	  }

	  @Override
	  public List<RecommendedItem> recommend(long userID, int howMany)
	      throws TasteException {
	    
	    return delegate.recommend(userID, howMany);
	  }

	  @Override
	  public List<RecommendedItem> recommend(long userID,
	                                         int howMany,
	                                         IDRescorer rescorer)
	      throws TasteException {
	    return delegate.recommend(userID, howMany);
	  }

	  @Override
	  public float estimatePreference(long userID, long itemID)
	      throws TasteException {
	    
	    return (float) delegate.estimatePreference(userID, itemID);
	  }

	  @Override
	  public void setPreference(long userID, long itemID, float value)
	      throws TasteException {
	    delegate.setPreference(userID, itemID, value);
	  }

	  @Override
	  public void removePreference(long userID, long itemID)
	      throws TasteException {
	    delegate.removePreference(userID, itemID);
	  }

	  @Override
	  public DataModel getDataModel() {
	    return delegate.getDataModel();
	  }

	  @Override
	  public void refresh(Collection<Refreshable> alreadyRefreshed) {
	    delegate.refresh(alreadyRefreshed);
	  }

	  

}
