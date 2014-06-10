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

			File file = new File("labeled_point.txt");

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);

			String sCurrentLine;
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
					bw.write(line+"\n");
				}
			}
			bw.close();

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
