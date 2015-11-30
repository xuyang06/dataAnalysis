package com.data.reader;

import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;

public class Recommend {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String path_str = "/home/cloudera/yelpDataSet/training_review.csv";
		LongFileDataModel alpha = new LongFileDataModel(path_str);
		alpha.formDataModel();
	    DataModel model = alpha.getDataModel();
	    long userID1 = 228055;
	    long itemID1 = 34439;
	    System.out.println(model.getPreferenceValue(userID1, itemID1));
	    Recommender mfRecommender = new MFBasedRecommender(model);
	    /*long userID = 5980; 
	    long itemID = 18718;
	    float rating = mfRecommender.estimatePreference(userID, itemID);
	    System.out.println(rating);*/
	    String test_path_str = "/home/cloudera/yelpDataSet/test_review.csv";
	    FileEstimator fileEstimator= new FileEstimator(test_path_str, alpha, mfRecommender);
	    System.out.println(fileEstimator.calMSE());
	    /*RecommenderEvaluator evaluator = 
	      new AverageAbsoluteDifferenceRecommenderEvaluator();

	    RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
	      @Override
	      public Recommender buildRecommender(DataModel model) throws TasteException {
	        try {
	          return new MFBasedRecommender(model);
	        } catch (IOException ioe) {
	          throw new TasteException(ioe);
	        }

	      }
	    };
	    double score = evaluator.evaluate(recommenderBuilder, null, model, 0.75, 0.1);
	    System.out.println(score);*/

		
	}

}
