package it.java.bigdata.seqfilesGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class CSVtoSeqFiles {
	public static void main( String[] args ) throws Exception{
		readHitsCSV(args[0]);
//		readHitsCSV("dataset.csv");
	}

	private static void readHitsCSV(String filename) throws Exception  {
		File f = new File(filename);		
		Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Writer writer = new SequenceFile.Writer(fs, configuration, new Path("seqfile"),Text.class, Text.class);
        
        BufferedReader reader = new BufferedReader(new FileReader(f));
        Text key = new Text();
        Text value = new Text();
        
        int c = 0;
        while(true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }
            line = line.replaceAll("\"\"", "\"null\"");
            String[] tokens = line.split(",");
            String category = tokens[22].equalsIgnoreCase("\"0\"")?"isNotSpider":"isSpider";
            key.set("/"+category+"/"+String.valueOf(c));
            value.set(line);
            writer.append(key, value);
            c++;
        }
        writer.close();
        reader.close();
	}
}
