package helpers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;


public class ExtractData  implements Main.Command {

	// args[0] = input path
	// args[1] = output path
	// args[2] = numSplits
	
	
	@Override
	public int execute(HashMap<String,String> params) {
		String inputFile = params.get( "input");
		String outputFile = params.get( "output" );
		String lines = params.get("lines");
		
		if( inputFile == null || outputFile == null || lines == null) {
			usage();
			return 0;
		}
		
		extract( inputFile, outputFile, lines);
		
		return 1;
	}
	
	@Override
	public void usage() {

		System.out.println( "** command=extract        - Extracts specified number of lines from source file to destination file:" );
		System.out.println( "   input=inputfile        : Input file to be processed" );
		System.out.println( "   output=outputfile      : New file is created here");
		System.out.println( "   lines=lines            : Number of lines from source file");
	}
	
	
	public static void main(String[] args) {
		extract(args[0], args[1], args[2]);
		
	}

	private static void extract(String input, String output, String numlines_string) {
		int numlines = Integer.parseInt(numlines_string);
		
		System.out.printf( "input [%s] output [%s] lines [%d]\n", input, output, numlines );

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);

		try {
			FileSystem fs = FileSystem.get(conf);

			Path inputPath = new Path(input);
			Path outputPath = new Path(output);

			BufferedReader br = new BufferedReader( new InputStreamReader(fs.open(inputPath)));
			BufferedWriter bw = new BufferedWriter( new OutputStreamWriter(fs.create(outputPath)));
			
			for( String line = br.readLine(); line != null && numlines > 0; line = br.readLine(), numlines-- ) {
				bw.write(line+"\n");
			}
			br.close();
			bw.close();
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
}


