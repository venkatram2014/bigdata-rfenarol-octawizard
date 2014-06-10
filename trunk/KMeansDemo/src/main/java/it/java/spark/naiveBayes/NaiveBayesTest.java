package it.java.spark.naiveBayes;

import it.java.spark.kmeans.JavaKMeans;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class NaiveBayesTest {

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext("local", "JavaKMeans",
				System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(JavaKMeans.class));

		JavaRDD<LabeledPoint> training = null; // training set
		JavaRDD<LabeledPoint> test = null; // test set

		final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);

		JavaRDD<Double> prediction =
				test.map(new Function<LabeledPoint, Double>() {
					@Override public Double call(LabeledPoint p) {
						return model.predict(p.features());
					}
				});
		JavaPairRDD<Double, Double> predictionAndLabel = 
				prediction.zip(test.map(new Function<LabeledPoint, Double>() {
					@Override public Double call(LabeledPoint p) {
						return p.label();
					}
				}));
		double accuracy = 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
			@Override public Boolean call(Tuple2<Double, Double> pl) {
				return pl._1() == pl._2();
			}
		}).count() / test.count();

	}

}
