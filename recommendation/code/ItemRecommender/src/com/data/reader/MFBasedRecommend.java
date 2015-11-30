package com.data.reader;

import java.io.File;
import java.io.IOException;
import java.util.List;


import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator;

public class MFBasedRecommend {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String path_str = "/home/cloudera/Documents/yelp_review_test.csv";
		AlphaItemFileDataModel alpha = new AlphaItemFileDataModel(path_str);
		alpha.formDataModel();
	    DataModel model = alpha.getDataModel();
	    RecommenderEvaluator evaluator =
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
	    System.out.println(score);
	}

}
