package it.spark.sparkKMeans;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;

/**
 * Example using MLLib KMeans from Java.
 */
public final class KMeansTraining {

	static class ParsePoint extends Function<String, double[]> {
		private static final long serialVersionUID = -8651957309737119456L;
		private static final Pattern SPACE = Pattern.compile(" ");
		
		@Override
		public double[] call(String line) {
			String[] tok = SPACE.split(line);
			double[] point = new double[tok.length-1];
			// parto dal secondo elemento della riga, il primo è l'id del vettore
			for (int i = 1; i < tok.length; ++i) {
				point[i-1] = Double.parseDouble(tok[i]);
			}
			id2vector.put(Integer.parseInt(tok[0]), point);
			return point;
		}
	}

	private static HashMap<Integer, double[]> id2vector;

	/**
	 * 
	 * @param args <input> <outputFile> <context[local, cluster]> <k> <iterations> <runs> 
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length < 6) {
			System.err.println(
					"Usage: JavaKMeans <input> <outputFile> <context[local | cluster]> <k> <iterations> [<runs>]");
			System.exit(1);
		}
		
		String input = args[0];
		String output = args[1]+"/k-means_out.txt";
		String master = args[2];
		int k = Integer.parseInt(args[3]);
		int iterations = Integer.parseInt(args[4]);
		int runs = 1;
		if (args.length > 5) {
			runs = Integer.parseInt(args[5]);
		}

		File outputFile = new File(output);
		if (outputFile.exists())
			outputFile.delete();
		// if file doesn't exists, then create it
		if (!outputFile.exists()) {
			outputFile.createNewFile();
		}

		FileWriter fw = new FileWriter(outputFile.getAbsoluteFile(), true);
		BufferedWriter bw = new BufferedWriter(fw);
		
		//TODO quando siamo nel cluster, cambiare il context di SPARK, non è local
		JavaSparkContext sc = new JavaSparkContext(master /*"local"*/, "JavaKMeans",
				System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(KMeansTraining.class));
		JavaRDD<String> lines = sc.textFile(input);	//"kmeans_data.txt"

		id2vector = new HashMap<Integer, double[]>();
		JavaRDD<double[]> points = lines.map(new ParsePoint());

		KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs);

		bw.write("---- Cluster centers ----\n");
		int cont = 0;
		for (double[] center : model.clusterCenters()) {
			bw.write("Cluster"+cont+" centroid: "+Arrays.toString(center)+"\n");
			cont ++;
		}
		
		double cost = model.computeCost(points.rdd());
		bw.write("\nCost: "+ cost+"\n");

		//per ogni punto di input, fai la predizione
//		List<double[]> iterable_points = points.collect();
//		bw.write("\n\tpoint\t\t-->\tcluster\n");
//		for (double[] point : iterable_points)
//			bw.write(Arrays.toString(point) +" --> "+model.predict(point)+"\n");
		for (Integer i : id2vector.keySet()){
			double[] point = id2vector.get(i);
			bw.write("vector id ["+i+"] --> "+model.predict(point)+"\n");
		}

		bw.close();
		System.exit(0);
	}
}
