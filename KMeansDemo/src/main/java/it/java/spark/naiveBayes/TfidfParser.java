package it.java.spark.naiveBayes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;

public class TfidfParser {

	public static void main(String[] args) {
		String input = "tfidf.txt";

		BufferedReader br = null;

		try {

			File trainfile = new File("train_labeled_points.txt");
			File testfile = new File("test_labeled_point.txt");

			// if file doesnt exists, then create it
			if (!trainfile.exists()) {
				trainfile.createNewFile();
			}
			// if file doesnt exists, then create it
			if (!testfile.exists()) {
				testfile.createNewFile();
			}

			FileWriter trainfw = new FileWriter(trainfile.getAbsoluteFile(), true);
			BufferedWriter trainbw = new BufferedWriter(trainfw);
			FileWriter testfw = new FileWriter(testfile.getAbsoluteFile(), true);
			BufferedWriter testbw = new BufferedWriter(testfw);

			String sCurrentLine;
			br = new BufferedReader(new FileReader(input));
			
			int numLines = 0;
			while ((sCurrentLine = br.readLine()) != null) {
				numLines ++;
			}
			
			int trainLines = numLines*60/100;
			int lines = 0;
			br = new BufferedReader(new FileReader(input));
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.contains("{")){
					String line;
					if (sCurrentLine.matches("Key: /isSpider/.*"))
						line = "1,";
					else
						line = "0,";
					int start = sCurrentLine.indexOf('{');
					String array_string = sCurrentLine.substring(start, sCurrentLine.length()-1);
					String[] records = array_string.split(",");
					for (String record : records){
						StringTokenizer tokenizer = new StringTokenizer(record, ":");
						tokenizer.nextToken();
						String double_string = tokenizer.nextToken();
						line += double_string+"\t";
					}
					
					if(lines < trainLines){
						trainbw.write(line+"\n");
					}else{
						testbw.write(line+"\n");
					}
					lines++;
				}
			}
			trainbw.close();
			testbw.close();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}

}
