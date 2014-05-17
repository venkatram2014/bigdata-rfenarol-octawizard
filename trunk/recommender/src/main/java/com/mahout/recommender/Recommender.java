package com.mahout.recommender;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class Recommender {
	
	public static void main (String[] args) throws IOException, TasteException{
		/* il DataModel costruisce la matrice(utente x item); 
		 * la singola casella è il rating che l'utente da all'oggetto */
		DataModel model = new FileDataModel(new File("dataset.txt"));
		
		UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		
		UserNeighborhood neigh = new ThresholdUserNeighborhood(0.1, similarity, model);
		
		UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neigh, similarity);
		/* recommend utente 2, 3 raccomandazioni */
		List<RecommendedItem> recommendations = recommender.recommend(2, 3);
		for (RecommendedItem r : recommendations)
			System.out.println(r);
		
		//TODO
		/* evaluate --> test 
		 * ItemBasedSimilarity*/
	}

}
