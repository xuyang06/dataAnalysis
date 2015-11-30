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
import org.apache.mahout.cf.taste.impl.recommender.svd.*;

public class MFBasedRecommender implements Recommender {

	  private Recommender delegate;
	  //private DataModel model;
	  //private MemoryIDMigrator thing2long = new MemoryIDMigrator();
	  //private MemoryIDMigrator date2long = new MemoryIDMigrator();

	  
	  public MFBasedRecommender(DataModel model)
	      throws TasteException, IOException {
		
		  Factorizer factorizer = new SVDPlusPlusFactorizer(model, 20, 10000);
		  delegate =
			        new SVDRecommender(model, factorizer);
	    /*UserSimilarity similarity = new EuclideanDistanceSimilarity(model);
	    UserNeighborhood neighborhood =
	        new NearestNUserNeighborhood(2, similarity, model);
	    
	    
	    delegate =
	        new GenericUserBasedRecommender(model, neighborhood, similarity);*/
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