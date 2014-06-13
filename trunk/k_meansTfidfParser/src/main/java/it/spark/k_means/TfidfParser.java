package it.spark.k_means;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringTokenizer;

public class TfidfParser {

	public static void main(String[] args) {
		String input = args[0]; // + "/tfidf.txt";

		BufferedReader br = null;

		try {
			File parsedFile = new File(args[1]/*"k_means_matrix.txt"*/);

			// if file doesn't exists, then create it
			if (!parsedFile.exists()) {
				parsedFile.createNewFile();
			}

			FileWriter fw = new FileWriter(parsedFile.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);

			String sCurrentLine;
			br = new BufferedReader(new FileReader(input));

			int max_tfidfVector_lenght = 0;
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.contains("{")){
					int start = sCurrentLine.indexOf('{');
					String array_string = sCurrentLine.substring(start, sCurrentLine.length()-1);
					String[] records = array_string.split(",");
					if(records.length > max_tfidfVector_lenght) 
						max_tfidfVector_lenght = records.length;
				}
			}
			br.close();

			br = new BufferedReader(new FileReader(input));
			while ((sCurrentLine = br.readLine()) != null) {
				if (sCurrentLine.contains("{")){
					String id_field = (sCurrentLine.split(": "))[1];
					String id = id_field.replaceAll(".*Spider/", "");
					String line = "";						
					int start = sCurrentLine.indexOf('{');
					String array_string = sCurrentLine.substring(start, sCurrentLine.length()-1);
					String[] records = array_string.split(",");
					for (String record : records){
						StringTokenizer tokenizer = new StringTokenizer(record, ":");
						tokenizer.nextToken();
						String double_string = tokenizer.nextToken();
						line += double_string+" ";
					}
					if(records.length<max_tfidfVector_lenght){
						int difference = max_tfidfVector_lenght-records.length;
						for(int i=0; i<difference; i++){
							line+="0.0"+" ";
						}
					}
					bw.write(id+" "+line+"\n");
				}
			}
			bw.close();
			br.close();
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
