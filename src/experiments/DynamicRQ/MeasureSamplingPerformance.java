package experiments.DynamicRQ;

import helpers.Main;

import index.RTree;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import partitioning.Common;
import helpers.Constants;
import helpers.SplitMergeInfo;

import core.CostEstimator;
import core.DynamicPartitioning;
import core.Partition;
import core.Solution;

import experiments.QWload;
import experiments.Stats;

import tools.ACETree2d;
import tools.ACETree2dSearch;
import tools.Rect;
import tools.Util;
import tools.Point;


public class MeasureSamplingPerformance implements Main.Command {
	
	public enum SampleType {
		NONE, FIRST, RANDOM, ACE
	};

	static Solution aqwa;
	static DynamicPartitioning aqwaPartitioning;

/*
	static Solution grid;
	static RTree<Partition> gridPartitionsRTree;  // used only for grid.

	static Solution staticTree;
	static RTree<Partition> kdPartitionsRTree;  // used only for static.
*/

	static BufferedWriter out;
	// static int numBatches;
	static int numBatches = 10;
	static int batchSize = 20;
	// static int batchSize = 1;
	static ArrayList<Partition> allQueries;

	//static String gridPath = "Grid/";
	//static String staticKdPath = "kd/";
	static String aqwaPath = "AQWA/";
	static boolean tempStopHere = true;
	
	
	@Override
	public void usage() {
		System.out.println( "** command=msample        - Measures sampling performance." );
		System.out.println( "   input=inputfile        : Input file to be processed" );
		System.out.println( "   counts=countsfile      : Count file where the counts are stored");
		System.out.println( "   numfiles=numfiles      : Number of partitions desired");
		System.out.println( "   sample=type            : sample type - one of NONE, FIRST, RANDOM");
		System.out.println( "   partition=boolean      : True or False");
	}
	
	private static void doUsage() {
		/* ugh - ugly - need to instantiate just to invoke the usage() :-( */
		MeasureSamplingPerformance msp = new MeasureSamplingPerformance();
		msp.usage();
	}
	
	@Override
	public int execute( HashMap<String,String> params) {
		String inputFile = params.get( "input");
		String countsFile = params.get( "counts" );
		String numFiles_string = params.get("numfiles");
		String sample_string = params.get("sample");
		String doPartition_string = params.get("partition");
		
		if( inputFile == null || countsFile == null || numFiles_string == null || sample_string == null || doPartition_string == null ) {
			usage();
			return 0;
		}
		
		doit( inputFile, countsFile, numFiles_string, sample_string, doPartition_string);
		
		return 1;
	}

	// args[0] = directory of input data.
	// args[1] = initial counts file
	// args[2] = number of files (initialPartitions)
	// args[3] = numBatches  - NOT USED CURRENTLY
	public static void main(String[] args) {
		// int numBatches = Integer.parseInt(args[3]);  // NOT USED CURRENTLY
		
		doit( args[0], args[1], args[2], args[3], args[4]);
		
	}
	
	public static SampleType getSampleType(String sample_string) {
		HashMap<String, SampleType> sampleD = new HashMap<String,SampleType>();
		sampleD.put("none", SampleType.NONE);
		sampleD.put("first", SampleType.FIRST);
		sampleD.put("random", SampleType.RANDOM);
		sampleD.put("ace", SampleType.ACE);
		
		sample_string = sample_string.toLowerCase();
		
		return sampleD.get(sample_string);
	}
	
	public static void doit(String inputFile, String countsFile, String numFiles_string, String sample_string, String doPartition_string) {
		
		int numFiles = Integer.parseInt(numFiles_string);
		SampleType sampleType = getSampleType(sample_string);
		if( sampleType == null) {
			System.out.printf( "unrecognized sample type - [%s]\n", sample_string);
			doUsage();
			return;
		}
		
		if( !doPartition_string.equalsIgnoreCase("true") && !doPartition_string.equalsIgnoreCase("false") ) {
			System.out.println( "partition has to be true or false");
			doUsage();
			return;
		}
		
		boolean doPartition = Boolean.parseBoolean( doPartition_string.toLowerCase());
		
		System.out.printf( "doPartition [%d] doPartition_string [%s]\n", doPartition ? 1 : 0, doPartition_string);
		
		System.out.printf( "Inside do it: %s, %s, %s, %s, %s\n", inputFile, countsFile, numFiles_string, sample_string, doPartition_string);

		CostEstimator costEstimator = new CostEstimator(null, null, Constants.gridWidth, Constants.gridHeight);
		costEstimator.updateCountsInGrid(countsFile);
		
		FileStatus[] files = null;
		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon, MeasureSamplingPerformance.class);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path inputPath = new Path(inputFile);
			files = fs.listStatus(inputPath);

			init(conf, files, numFiles, costEstimator, numBatches, sampleType, doPartition);

			// Exec some queries on initial setup. AQWA is redundant here.		
			execAll(conf, fs, sampleType); /* execAll(conf, fs); execAll(conf, fs); */

			if( MeasureSamplingPerformance.tempStopHere ) {
				System.out.println( "Returning...\n");
				return;
			}
			
			return;
			
			/*
			int remaining = executeTillNoChange(conf, fs, numBatches / files.length - 3);
			System.out.println("Remaining = " + remaining);
			for (int b = 0; b < remaining; b++) {
				AQWAUpdateCountsOnly(conf, fs);
			}

			for (int i = 1; i < files.length; i++) {
				System.out.println("Appending " + files[i].getPath().toString());
				System.out.println("Appending to static kd tree");
				Common.execAppend(files[i].getPath().toString(), "tmpCounts", staticKdPath, staticTree);
				costEstimator.updateCountsInGrid("tmpCounts");

				System.out.println("Appending to static grid");
				Common.execAppend(files[i].getPath().toString(), "tmpCounts", gridPath, grid);

				System.out.println("Appending to AQWA");
				Common.execAppend(files[i].getPath().toString(), "tmpCounts", aqwaPath, aqwa);

				// Exec some queries
				execAll(conf, fs); execAll(conf, fs); execAll(conf, fs);
				remaining = executeTillNoChange(conf, fs, numBatches / files.length - 3);
				System.out.println("Remaining = " + remaining);
				for (int b = 0; b < remaining; b++) {
					AQWAUpdateCountsOnly(conf, fs);
				}
				
				// Update the partitioning layout for future appends
				aqwa = aqwaPartitioning.getSolution();
			}
			*/

		} catch (Exception c) {
			System.err.println(c);
		}
	}

	private static int executeTillNoChange(JobConf conf, FileSystem fs, int repeat) {
		int remaining = repeat;
		for (int b = 0; b < repeat; b++) {
			boolean stop = execAQWAOnly(conf, fs);
			remaining --;
			if (stop) {
				// Execute three more then break if all trigger no dynamic updates
				remaining--;
				if (!execAQWAOnly(conf, fs)) {					
					continue;
				}
				remaining--;
				if (!execAQWAOnly(conf, fs)) {					
					continue;
				}
				remaining--;
				if (!execAQWAOnly(conf, fs)) {					
					continue;
				}
				break;
			}
		}
		return remaining;
	}

	private static void init(JobConf conf, FileStatus[] files, int numFiles, CostEstimator costEstimator, int numBatches, SampleType sampleType, boolean doPartition) {
		conf.set("mapreduce.job.jvm.numtasks", "-1");				// Infinity			
		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setMapperClass(RQueryMap.class);
		conf.setReducerClass(RQueryReduce.class);
		conf.setCombinerClass(RQueryReduce.class);
		conf.setInputFormat(TextInputFormat.class);

		// String statsFileName = "/home/hduser/aqwa/expResults/QueryPerformance.csv";		
		String statsFileName = "QueryPerformance.csv";		
		try {
			out = new BufferedWriter(new FileWriter(statsFileName, true));
			/* Aravind  - Only AQWA */
			/*
			out.write("Grid Elapsed Time, Grid Mappers Time, Grid HDFS Bytes Read, Grid number of Records,"
					+ "kd Elapsed Time, kd Mappers Time, kd HDFS Bytes Read, kd number of Records,"
					+ "AQWA Elapsed Time, AQWA Mappers Time, AQWA HDFS Bytes Read, AQWA number of Records,"
					+ "time for split merge  \r\n");
			*/	
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
			Date now = new Date();
			String strDate = sdf.format(now);
			
			out.write( "***********************\r\n");
			out.write( "test started at " + strDate + "\r\n" );
			out.write( "sample type is: " + String.valueOf(sampleType) + "\r\n" );
			
			out.write( "AQWA Elapsed Time, AQWA Mappers Time, AQWA HDFS Bytes Read, AQWA number of Records,"
						+ "time for split merge, Number of Partitions  \r\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Generating Interleaved Queries
		ArrayList<QWload> wLoad = new ArrayList<QWload>();
		// Four hot spots		
		
		System.out.printf( "Creating [%d] queries\n", numBatches * batchSize );
		
		wLoad.add(new QWload(850, 600, numBatches * batchSize));
		
//		wLoad.add(new QWload(500, 500, numBatches * batchSize/3));
//		wLoad.add(new QWload(600, 600, numBatches * batchSize/3));
//		wLoad.add(new QWload(700, 700, numBatches * batchSize/3));
//		wLoad.add(new QWload(800, 700, numBatches * batchSize/3));
		allQueries = QWload.getInterleavedQLoad(wLoad, 100);

		// Initialize partitioning
		System.out.println("Initial Partitioning");
		// AQWA
		aqwaPartitioning = new DynamicPartitioning(costEstimator, numFiles, 1000);
		aqwa = new Solution();
		for (Partition p : aqwaPartitioning.initialPartitions()) {
			aqwa.addPartition(p);
		}
		System.out.println("Initializing AQWA");
		System.out.println("path = " + files[0].getPath().toString());
		
		
		if( doPartition ) {
			System.out.println( "**** Creating partitions...");
			Common.execPartitioning(files[0].getPath().toString(), aqwaPath, aqwa);
		} else {
			System.out.println( "**** not creating partitions!!!");			
		}
		
		// ---------------------------
		// GRID
		/*
		grid = Common.initGridPartitioning(numFiles);
		gridPartitionsRTree = new RTree<Partition>(10, 2, 2);
		for (Partition p : grid.getPartitions()) {
			gridPartitionsRTree.insert(p.getCoords(), p.getDimensions(), p);
		}
		System.out.println("Initializing Grid");
		Common.execPartitioning(files[0].getPath().toString(), gridPath, grid);

		// ---------------------------
		// Static kd
		staticTree = Common.initKDPartitioning(numFiles, costEstimator);
		kdPartitionsRTree = new RTree<Partition>(10, 2, 2);
		for (Partition p : staticTree.getPartitions()) {
			kdPartitionsRTree.insert(p.getCoords(), p.getDimensions(), p);
		}
		System.out.println("Initializing Static kdtree");
		Common.execPartitioning(files[0].getPath().toString(), staticKdPath, staticTree);
		*/		
	}

	// return true when u should break
	private static boolean execAQWAOnly(JobConf conf, FileSystem fs) {
		if (allQueries.size() < batchSize) {
			System.out.println("Ran out of queries");		
			return true;
		}
		try {
			// Get a batch
			ArrayList<Partition> batch = new ArrayList<Partition>();
			for (int qId = 0; qId < batchSize; qId++) {
				batch.add(allQueries.remove(0));
			}

			// Write 0s for Grid:			
			Stats stats = new Stats();
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");

			// Write 0s for Static kd:
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();

			// Run on AQWA:
			ArrayList<List<Partition>> aqwaPartitions = new ArrayList<List<Partition>>();
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				System.out.printf( "Query Coordinates - %f %f \n", query.getCoords()[0], query.getCoords()[1]);
				
				for (Partition p : aqwaPartitioning.partitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}
				aqwaPartitions.add(partitions);

				// No split and merge here
				aqwaPartitioning.processNewQueryUpdateStatsOnly(query);
			}

			stats = ExecRangeJob.exec(conf, fs, batch, aqwaPath, "temp_query_results", aqwaPartitions);

			double splitTime = 0;
			ArrayList<SplitMergeInfo> splits = aqwaPartitioning.processNewQuery(batch.get(0)); 
			if (splits.size() > 0) {
				long startTime = System.currentTimeMillis();
				for (SplitMergeInfo splitInfo : splits) {
					System.out.println("Splitting...");					
					SplitMergeInfo.splitPartitions(aqwaPath, splitInfo);					
				}
				long endTime = System.currentTimeMillis();
				splitTime += (endTime - startTime) / 1000; //write time in milliseconds
			}
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", " + 
								splitTime + ", " + stats.numPartitions + "\r\n");
			out.flush();			
			if (splits.size() > 0) {
				return false;
			} else {
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	private static void AQWAUpdateCountsOnly(JobConf conf, FileSystem fs) {
		if (allQueries.size() < batchSize) {
			System.out.println("Ran out of queries");		
			return;
		}
		// Get a batch
		ArrayList<Partition> batch = new ArrayList<Partition>();
		for (int qId = 0; qId < batchSize; qId++) {
			batch.add(allQueries.remove(0));
		}
		
		// Dump zeros
		Stats stats = new Stats();
		try {
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", " + 0 + "\r\n");
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Run on AQWA:
		for (Partition query : batch) {
			// No split and merge here
			aqwaPartitioning.processNewQueryUpdateStatsOnly(query);
		}		
	}
	
	private static List<Partition> getSampledPartitions(List<Partition> partitions, Partition query, SampleType sampleType) {
		if( sampleType == SampleType.NONE ) {
			return partitions;
		} else if( sampleType == SampleType.FIRST) {
			List<Partition> newPartitions = new ArrayList<Partition>();
			newPartitions.add( partitions.get(0));
			return newPartitions;
		} else if( sampleType == SampleType.RANDOM) {
			int index = (int)(Math.random() * partitions.size());
			List<Partition> newPartitions = new ArrayList<Partition>();
			newPartitions.add(partitions.get(index));
			return newPartitions;
		} else if( sampleType == SampleType.ACE) {
			List<Partition> newPartitions = new ArrayList<Partition>();
			/*
			 * we have a bug where the number is less than 4.  we will use this until we fix it.
			 */
			if( partitions.size() < 4) {
				int index = (int)(Math.random() * partitions.size());
				newPartitions.add(partitions.get(index));
				return newPartitions;
			}
			
			/* 
			 * for now, we will construct the tree right here.
			 * we should construct the tree when we build the R Tree index and do search instead.
			 */
			ArrayList<tools.Point>points = new ArrayList<tools.Point>();
			for( Partition p : partitions ) {
				points.add( new Point(p.getLeft(), p.getBottom()));
			}
			
			ACETree2d aceTree = new ACETree2d(points);
			tools.Rect queryRect = new tools.Rect(query.getLeft(), query.getBottom(), query.getRight(), query.getTop());
			ACETree2dSearch aceTreeSearch = new ACETree2dSearch(aceTree, queryRect);
			
			ArrayList<Point> result = null;
			
			while( (result == null || result.size() == 0) && !aceTreeSearch.done() ) {
				result = aceTreeSearch.search();
			}
			
			/* 
			 * ugly, but the volume is so low we can do this.  need to fix the performance later.
			 * also, need to consider how to handle Rect instead of points. 
			 */
			for( Partition p : partitions ) {
				Point pt = new Point(p.getLeft(), p.getBottom());
				if( result.contains(pt)) {
					newPartitions.add(p);
				}
			}
			
			Util.log( Util.None, "Size of points result [%d] Returning number of partitions -> %d\n", result.size(), newPartitions.size() );
			
			if( newPartitions.size() == 0 ) {
				Util.log( Util.None,  "%s\n", points );
				Util.log( Util.None, "Query Rect is %s\n", queryRect );

				/* 
				 *  add one random partition
				 * 
				 *  need to figure out when this happens.
				 *  one possibility is that all partitions are partially covered by the query.  We may not be able to catch it since
				 *  we are using only the left/bottom coordinate.
				 */
				int index = (int)(Math.random() * partitions.size());
				newPartitions.add(partitions.get(index));
			}
			
			
			return newPartitions;
			
		} else {
			System.out.println( "not a valid option for Sampling the partitions!!");
			return null;
		}
	}

	// return true when u should break
	private static boolean execAll(JobConf conf, FileSystem fs, SampleType sampleType) {
		if (allQueries.size() < batchSize) {
			System.out.println("Ran out of queries");		
			return true;
		}
		try {
			// Get a batch
			ArrayList<Partition> batch = new ArrayList<Partition>();
			for (int qId = 0; qId < batchSize; qId++) {
				batch.add(allQueries.remove(0));
			}

			/*
			// Run on Grid:
			ArrayList<List<Partition>> gridPartitions = new ArrayList<List<Partition>>();			
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : gridPartitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}					
				gridPartitions.add(partitions);					
			}
			Stats stats = ExecRangeJob.exec(conf, fs, batch, gridPath, "temp_query_results", gridPartitions);
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();
			//		if (staticStats.elapsedTime > 0) continue;

			// Run on Static kd:
			ArrayList<List<Partition>> kdPartitions = new ArrayList<List<Partition>>();			
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : kdPartitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}					
				kdPartitions.add(partitions);					
			}

			stats = ExecRangeJob.exec(conf, fs, batch, staticKdPath, "temp_query_results", kdPartitions);
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();
			*/

			// Run on AQWA:
			ArrayList<List<Partition>> aqwaPartitions = new ArrayList<List<Partition>>();
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();

				/* 
				 System.out.printf( "Query -> Bottom/Left: %d %d Top/Right: %d %d\n", query.getBottom(), query.getLeft(), query.getTop(), query.getRight());
				 */
				
				for (Partition p : aqwaPartitioning.partitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}
				
				partitions = getSampledPartitions(partitions, query, sampleType);
				
				aqwaPartitions.add(partitions);

				// No split and merge here
				aqwaPartitioning.processNewQueryUpdateStatsOnly(query);
			}

			Stats stats = ExecRangeJob.execSerial(conf, fs, batch, aqwaPath, "temp_query_results", aqwaPartitions);

			double splitTime = 0;
			/*
			ArrayList<SplitMergeInfo> splits = aqwaPartitioning.processNewQuery(batch.get(0)); 
			if (splits.size() > 0) {
				long startTime = System.currentTimeMillis();
				for (SplitMergeInfo splitInfo : splits) {
					System.out.println("Splitting...");					
					SplitMergeInfo.splitPartitions(aqwaPath, splitInfo);					
				}
				long endTime = System.currentTimeMillis();
				splitTime += (endTime - startTime) / 1000; //write time in milliseconds
			}
			*/
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", " + splitTime + "\r\n");
			out.flush();
			/*
			if (splits.size() > 0) {
				return false;
			} else {
				return true;
			}
			*/
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}
	
	private static void execAllNoAQWA(JobConf conf, FileSystem fs) {
		if (allQueries.size() < batchSize) {
			System.out.println("Ran out of queries");		
			return;
		}
		/*
		try {
		*/
			// Get a batch
			ArrayList<Partition> batch = new ArrayList<Partition>();
			for (int qId = 0; qId < batchSize; qId++) {
				batch.add(allQueries.remove(0));
			}

			/*
			// Run on Grid:
			ArrayList<List<Partition>> gridPartitions = new ArrayList<List<Partition>>();			
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : gridPartitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}					
				gridPartitions.add(partitions);					
			}
			Stats stats = ExecRangeJob.exec(conf, fs, batch, gridPath, "temp_query_results", gridPartitions);
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();
			//		if (staticStats.elapsedTime > 0) continue;

			// Run on Static kd:
			ArrayList<List<Partition>> kdPartitions = new ArrayList<List<Partition>>();			
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : kdPartitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}					
				kdPartitions.add(partitions);					
			}

			stats = ExecRangeJob.exec(conf, fs, batch, staticKdPath, "temp_query_results", kdPartitions);
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();
			*/

			// Avoid on AQWA. Just update counts
			for (Partition query : batch) {
				// No split and merge here
				aqwaPartitioning.processNewQueryUpdateStatsOnly(query);
			}
			
			/*
			stats = new Stats();

			double splitTime = 0;			
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", " + splitTime + "\r\n");
			out.flush();
			*/			
		/*
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}

}
