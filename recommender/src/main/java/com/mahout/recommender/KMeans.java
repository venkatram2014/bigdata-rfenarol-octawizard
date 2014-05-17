package com.mahout.recommender;


import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;

public class KMeans {
	
	public static void main (String[] args){
		// run the CanopyDriver job
		CanopyDriver.runJob("testdata", "output"
		ManhattanDistanceMeasure.class.getName(), (float) 3.1, (float) 2.1, false);

		// now run the KMeansDriver job
		KMeansDriver.runJob("testdata", "output/clusters-0", "output",
		EuclideanDistanceMeasure.class.getName(), "0.001", "10", true);
	}

}
