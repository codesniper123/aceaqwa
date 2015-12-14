package partitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import partitioning.*;
import helpers.Constants;
import helpers.Main;

import java.util.HashMap;
import java.util.List;

import core.CostEstimator;
import core.DynamicPartitioning;
import core.GreedyGR;
import core.Partition;
import core.Solution;

public class PartitioningDriver implements Main.Command {

	@Override
	public void usage() {
		System.out.println( "** command=partition      - creates the partition from the given input file. " );
		System.out.println( "   input=input_directory  : contains the raw files to be partitioned / indexed"  );
		System.out.println( "   output=index_directory : directory where the partition / index files are stored");
		System.out.println( "   k=k_value              : still trying to understand if are using this" );
		System.out.println( "   grid=string_with_grid  : If this string has the word grid in it, we will build a grid index.");
		System.out.println( "   counts=counts_file     : Specify a file that has the initial counts");
	}
	
	@Override
	public int execute( HashMap<String,String> params) {
		String inputDir = params.get( "input");
		String indexDir = params.get( "output" );
		String grid = params.get("grid");
		String k_string = params.get("k");
		String initialCounts = params.get("counts");
		
		if( inputDir == null || indexDir == null || grid == null || k_string == null || initialCounts == null ) {
			usage();
			return 0;
		}
		
		doit( inputDir, grid, k_string, indexDir, initialCounts );
		
		return 1;
	}


	// args[0] is the input dir, e.g., osm/all
	// args[1] is a flag that if contains "grid", we will create a grid
	// args[2] is k
	// arg[3] is the index folder
	public static void main(String[] args) {

		if (args.length < 4) {
			System.err.println("Invalid parameters.");
		}

		int i = 0;
		for(String s : args) {
			System.out.println("param " + String.valueOf(i) + ":" + s);			
			i++;
		}
		
		doit(args[0], args[1], args[2], args[3], args[4] );
	}
	
	public static void doit(String inputDir, String grid, String k_string, String indexDir, String initialCounts )
	{
		String flag = grid;
		int k = Integer.parseInt(k_string);

		System.out.println("inputDir: " + inputDir);
		System.out.println("flag: " + flag);
		System.out.println("k: " + String.valueOf(k));
		System.out.println("indexDir: " + indexDir);
		System.out.println("counts: " + initialCounts);

		Solution layout;
		if (flag.toLowerCase().contains("grid")) {
			System.out.println("");
			// This is the spatial hadoop baseline
			layout = Common.initGridPartitioning(k);	
		}
		else {
			// These are the initial partitions for the dynamic partitioning afterwards.
			// You have to redo this partitioning every time you repeat the experiment, e.g., for different k.
			// This can also be used as the static k-d tree which is one of the baseline
			
			// String initialCounts = "Twitter0Counts";
			CostEstimator costEstimator = new CostEstimator(null, null, Constants.gridWidth, Constants.gridHeight);
			costEstimator.updateCountsInGrid(initialCounts);
			layout = Common.initKDPartitioning(k, costEstimator);			
		}
		Common.execPartitioning(inputDir, indexDir, layout);
	}
	
}
