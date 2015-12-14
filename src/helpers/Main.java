package helpers;

import java.util.HashMap;
import java.util.Map;

public class Main {
	static HashMap<String, String> commands;
	
	public static void init() {
		commands = new HashMap<String,String>();
		commands.put("counts", 		"helpers.CountInGrid");
		commands.put("partition",  	"partitioning.PartitioningDriver");
		commands.put("extract", 	"helpers.ExtractData");
		commands.put("split", 		"partitioning.SplitData");
		commands.put("msample", 	"experiments.DynamicRQ.MeasureSamplingPerformance");
	};
	
	public interface Command {
		public int execute(HashMap<String,String> params);
		public void usage();
	}

	public static void usage() {
		System.out.println( "Usage: hadoop jar runaqwa command=xx <params>");
		System.out.println("");
		for( Map.Entry<String,String> entry : commands.entrySet() ) {
			try {
				Class <?> theClass = Class.forName( entry.getValue() );
				Main.Command command = (Main.Command)theClass.newInstance();
				command.usage();
				System.out.println("");
			} catch(ClassNotFoundException | InstantiationException | IllegalAccessException e ) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String args[]) {
		init();
	
		if( args.length == 0 ) {
			usage();
			return;
		}
		
		HashMap <String, String> params = new HashMap<String, String>();
		
		for( String arg : args) {
			String tokens[] = arg.split("=");
			
			if( tokens.length != 2) {
				System.out.format( "Incorrect format for parameter - please use name=value format: %s\n", arg);
			}
			else {
				params.put( tokens[0], tokens[1] );
			}
		}
		
		String commandString = params.get("command");
		if( commandString == null ) {
			System.out.println( "no command string found...");
			usage();
			return;
		}

		String commandClassName = commands.get( params.get( "command" ));

		if( commandClassName == null ) {
			System.out.printf( "Unrecognized command: \"%s\" \n", commandString );
		} else {
			try {
				Class <?> theClass = Class.forName( commandClassName );
				Main.Command command = (Main.Command)theClass.newInstance();
			    command.execute(params);
			} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
		}
		
	}

}


