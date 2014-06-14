package it.java.bigdata.sparkNaiveBayes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class NaiveBayesTest {

	static class ParsePoint extends Function<String, LabeledPoint> {
		private static final long serialVersionUID = -7288037060883263596L;
		private static final Pattern COMMA = Pattern.compile(",");
		private static final Pattern SPACE = Pattern.compile("\t");

		@Override
		public LabeledPoint call(String line) {
			String[] parts = COMMA.split(line);
			double y = Double.parseDouble(parts[0]);
			String[] tok = SPACE.split(parts[1]);
			double[] x = new double[tok.length];
			for (int i = 0; i < tok.length; ++i) {
				x[i] = Double.parseDouble(tok[i]);
			}
			return new LabeledPoint(y, x);
		}
	}


	public static void main(String[] args) throws IOException {
		JavaSparkContext sc = new JavaSparkContext("local", "NaiveBayesTest",
				System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(NaiveBayesTest.class));

		JavaRDD<String> trainingdata = sc.textFile(args[0] + "/train_labeled_points.txt");
		JavaRDD<LabeledPoint> trainingPoints = trainingdata.map(new ParsePoint()).cache();
		JavaRDD<String> testdata = sc.textFile(args[0] + "/test_labeled_point.txt");
		JavaRDD<LabeledPoint> testPoints = testdata.map(new ParsePoint()).cache();
		
		FileWriter fw = new FileWriter("bayes_output.txt", true);
		BufferedWriter bw = new BufferedWriter(fw);

		final NaiveBayesModel model = NaiveBayes.train(trainingPoints.rdd(), 1.0);
		
		JavaRDD<Double> prediction =
				testPoints.map(new Function<LabeledPoint, Double>() {
					@Override public Double call(LabeledPoint p) {
						return model.predict(p.features());
					}
				});
		
		JavaPairRDD<Double, Double> predictionAndLabel = 
				prediction.zip(testPoints.map(new Function<LabeledPoint, Double>() {
					@Override public Double call(LabeledPoint p) {
						return p.label();
					}
				}));
		
		for(Double d : prediction.collect()){
			//System.out.println(d);
		}
		
//		JavaPairRDD<Double, List<Double>> class2list = predictionAndLabel.groupByKey();
//		List<Tuple2<Double, List<Double>>> class_list = class2list.collect();
//		for (Tuple2<Double, List<Double>> t : class_list){
//			System.out.println("CLASSE: "+t._1+"\tCOUNT: "+ t._2.size());
//		}
		
		Map<Double, Object> mappa = predictionAndLabel.countByKey();
		for(Double key : mappa.keySet()){
//			l.add("CLASSE: "+key.intValue() + "\tCOUNT: " + mappa.get(key));
		}
		bw.write("---- confusion matrix ----\n");
		bw.write("isSpider(=1)\t isNotSpider(=0)\n");
		Long isSpider = (Long) mappa.get(1.0);
		if (isSpider == null)
			isSpider = 0L;
		Long isNotSpider = (Long) mappa.get(0.0);
		if (isNotSpider == null)
			isNotSpider = 0L;
		bw.write(isSpider + "\t\t" + countSpiderError(predictionAndLabel)+"\t| isSpider = 1\n");
		bw.write(countNotSpiderError(predictionAndLabel) + "\t\t"+isNotSpider+"\t| isNotSpider = 0\n");
		
//		double count = predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
//			@Override public Boolean call(Tuple2<Double, Double> pl) {
//				System.out.println(pl._1() + "\t" + pl._2());
//				return pl._1() == pl._2();
//			}
//		}).;
		double count = 0;
		for (Tuple2<Double, Double> t : predictionAndLabel.collect()){
			if (t._1().equals(t._2()))
				count ++;
		}
		double accuracy = 1.0 * count/ testPoints.count();
		
		
		bw.write("\nGeneral accuracy: " + accuracy);

		bw.close();
	}


	private static Integer countSpiderError(
			JavaPairRDD<Double, Double> predictionAndLabel) {
		int count = 0;
		for (Tuple2<Double, Double> t : predictionAndLabel.collect()){
			if (t._2.intValue() == 1 && t._1.intValue() == 0)
				count ++;
		}
		return count;
	}


	private static Integer countNotSpiderError(
			JavaPairRDD<Double, Double> predictionAndLabel) {
		int count = 0;
		for (Tuple2<Double, Double> t : predictionAndLabel.collect()){
			if (t._2.intValue() == 0 && t._1.intValue() == 1)
				count ++;
		}
		return count;
	}

}
