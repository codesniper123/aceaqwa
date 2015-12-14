/*
 * tbd - need a drawing like: https://www.cs.umd.edu/class/spring2008/cmsc420/L19.kd-trees.pdf
 * tbd - need to fix the drawing we have and be able to draw the rectangle so we can see the solution easily
 * 
 */

package tools;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

// import de.erichseifert.vectorgraphics2d.SVGGraphics2D;

public class ACETree2d {
	protected static boolean DEBUG = true;
	
	/* How was each node sorted by */
	protected static final int SplitOnX = 1;
	protected static final int SplitOnY  = 2;
	
	protected ArrayList<Point> points;
	protected static int LOG_LEAF_SIZE = 1;		// aribtrary...
	protected int height;
	protected Node root;
	
	/* we store the points in the Record structure so we can assign section and leafID */
	ArrayList<Record> theRecords;
	
	/* we need to stop at a higher level InnerNode and assign a leaf.  This means, we need:
	 * a) a global structure that we can refer to, given the leaf Index - "Leaves" is this structure.
	 * b) Every InnerNode needs to know the Leaf IDs under it.
	 */
	protected ArrayList<LeafNode> leaves;

	protected static String getSplitOnStr(int splitOn ) {
		return splitOn == SplitOnX ? "x" : "y";
	}
	
	/* inner classes */
	interface Node {
		public void doDot(PrintWriter pw, int nodeNumber);
		public Range getValueRange();
		public String getRangeAsString();
		public Range getLeafIDs();
	}
	
	class InternalNode implements Node {
		/* how was this node sorted by - X or Y */
		protected int splitOn;
		
		protected int medianIndex;
		
		protected Point p; 					/* either X or Y is the key based on sortedBy */
		
		/* range of values under this inner node 
		 * this represents the range of splitting by the parent node
		 * for example, if the parent node split on X, then this range represents the X values of all points under this node 
		 * note that this node itself will split on Y.  (Except for the root node)
		 * In a tree, the range is drawn on the segment from the parent node to the child node 
		 */
		protected Range valueRange;			
		protected Range leafIDs;			/* range of leaf ids under this node */
		
		protected Node left;
		protected Node right;
		
		public InternalNode(int medianIndex, Point p, Range r, int splitOn) {
			this.medianIndex = medianIndex;
			this.p = p;
			this.valueRange = r;
			this.splitOn = splitOn;
			this.left = this.right = null;
			this.leafIDs = new Range();
		}
		
		@Override
		public Range getLeafIDs() {
			return leafIDs;
		}


		@Override
		public String getRangeAsString() {
			return String.format( "%d-%d", this.valueRange.begin, this.valueRange.end );
		}
		
		@Override
		public Range getValueRange() { 
			return this.valueRange;
		}
		
		@Override
		public String toString() {
			return String.format( "(%d %d) %s %d", 
									p.x, p.y, getSplitOnStr(splitOn), splitOn == SplitOnX ? p.x : p.y  );			
		}
		
		@Override
		public void doDot(PrintWriter pw, int nodeNumber) {
			pw.format("Node%d [width=1 height=1 label =\"%s\"]\n", nodeNumber, toString() );
		}
		

	}
	
	/*
	 * This class encapsulates each section in a LeafNode.  Note that the LeafNode contains an array of LeafSections.
	 * 
	 * Each LeafSection has a range and type (X or Y) associated with it. 
	 * Each LeafSection also has points assigned to it.
	 */
	class LeafSection {
		/* note that the split happens at the parent level.  the range of a child is thus maintained within the child node. 
		 * we should call this valueRange to be consistent with the InternalNode... 
		 */
		protected Range r;		
		
		/* are we even using this?  It may not make sense for a Leaf Node */
		protected int splitOn; 	
		ArrayList<Point> points;
		
		/* I do not like the "splitOn" term - think of this as type of dimension -> X or Y */
		public LeafSection(Range r, int splitOn) {
			this.r = r;
			this.splitOn = splitOn;
			points = new ArrayList<Point>();
		}
	}
	
	class LeafNode implements Node {
		Range r;
		ArrayList<LeafSection> leafSections;
		int leafID;
		
		public LeafNode(Range r, ArrayList<Range> xranges, ArrayList<Range> yranges) {
			this.r = r;
			this.leafSections = new ArrayList<LeafSection>();
			this.leafID = -1;
			
			/* create sections out of the ranges */
			/* the first one is X (decided by the algorithm) */
			int xIndex = 0;
			this.leafSections.add( new LeafSection( xranges.get(xIndex++), SplitOnX));
			
			/* now add alternatively, but start with X again */
			ArrayList<Range> ranges = xranges;
			int splitOn = SplitOnX;
			
			for( int yIndex = 0;  xIndex < xranges.size() || yIndex < yranges.size(); ){
				if( splitOn == SplitOnX ) {
					this.leafSections.add(new LeafSection(xranges.get(xIndex++), SplitOnX));
					splitOn = SplitOnY;
				} else if( splitOn == SplitOnY ) {
					this.leafSections.add(new LeafSection(yranges.get(yIndex++), SplitOnY));
					splitOn = SplitOnX;
				}
			}
		}
		
		void addRecord(int sectionID, Point p) {
			assert sectionID >= 0 && sectionID < leafSections.size();
			LeafSection leafSection = leafSections.get(sectionID);
			leafSection.points.add(p);
		}
		
		@Override
		public Range getLeafIDs() {
			/* should we remember this to prevent an allocation? */
			return new Range(leafID, leafID);
		}

		
		/*
		 *  We need the output to be in this form:
		 *  
		 *  struct1 [label="{a|{b|c}}|{d|{e|f}}"];
		 *  
		 *  for the DOT graphviz to arrange these as desired.
		 *
		 */
		@Override
		public void doDot(PrintWriter pw, int nodeNumber) {
			// pw.format( "Node%d [shape=rectangle width=3 height=3 label=\"%s\"]\n", nodeNumber, toString());
			pw.format( "Node%d [shape=rectangle width=0.5 height=0.5 label=\"%d\"]\n", nodeNumber, leafID);
			
			pw.format( "struct%d [shape=record label=\"", nodeNumber*leafSections.size()+1);
			
			boolean firstSection = true;
			for( LeafSection section : leafSections ) {
				pw.format( "%c{%s%d-%d|{", firstSection == true ? ' ' : '|', getSplitOnStr(section.splitOn), section.r.begin, section.r.end );
				boolean firstElement = true;
				for( Point p : section.points ) {
					pw.format( "%c(%d-%d)", firstElement == true ? ' ' : '|', p.x, p.y);
					firstElement = false;
				}
 				pw.format("}}" );
				firstSection = false;
			}
			
			pw.format( "\"]\n" );
			
			pw.format( "Node%d -> struct%d\n", nodeNumber, nodeNumber*leafSections.size()+1);
		}

		@Override
		public String getRangeAsString() {
			return String.format( "%d-%d", this.r.begin, this.r.end );
		}
		
		@Override
		public Range getValueRange() {
			return r;
		}
		
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			
			sb.append( String.format( "leafID = %d\n",  this.leafID));
			/*
			for( LeafSection leafSection : this.leafSections ) {
				sb.append( String.format( "%s %d-%d\n",  
										ACETree2d.getSplitOnStr(leafSection.splitOn), leafSection.r.begin, leafSection.r.end ));
			}
			 */ 
			return sb.toString();
		}
	}
	
	/* we store the section and the leaf number in this class */
	class Record {
		protected Point p;			/* the "value" of the record */
		protected int section;		/* note that the number of sections = height of the tree */
		protected int leafID;		/* leaves are numbered from left to right in the tree */
		
		public Record(Point p, int section, int leafID) {
			this.p = p;
			this.section = section;
			this.leafID = leafID;
		}
	}
	
	
	public ACETree2d(ArrayList<Point> points) {
		this.points = points;

		/* calculate the height of the tree */
		this.height = Util.getCeilingLog2(this.points.size()) - ACETree2d.LOG_LEAF_SIZE;
		Util.log( Util.None, "ACETree - size of arr [%d] height [%d]\n", this.points.size(), this.height);
		
		assert this.height > 0;
		
		/*
		 * The goal is to create internal nodes leading up to leaf nodes.
		 * 
		 * - First, we sort on the X value of the point and choose the median.  This is our root.
		 * - We split the points on the median.
		 * - Now, for each side of the partition, we sort based on the Y value and choose the median.  
		 * - This is our second internal node.
		 * - We repeat the process until we hit the leaf nodes.
		 */
		
		/* these are used to maintain a contiguous range in the Internal nodes which are ultimately useful in the LeafSections */
		ArrayList<Range> xranges = new ArrayList<Range>();
		ArrayList<Range> yranges = new ArrayList<Range>();

		Util.log( Util.Verbose, "constructor - before sorting on X\n");
		Util.log( Util.Verbose, "%s", points);
		
		/* sort the list on X first */
		Collections.sort( points, compareByX );

		Util.log( Util.Verbose, "constructor - after sorting\n");
		Util.log( Util.Verbose, "%s", points);

		/* since we start with SplitOnX, create the range for the xranges */
		Range r = new Range( points.get(0).x, points.get(points.size()-1).x);
		
		/* and push it in xranges */
		xranges.add(r);
		
		root = constructPhase1( 1, SplitOnX, points, 0, points.size() - 1, r, xranges, yranges);

		/* now do phase2 - section and leaf assignment */
		constructPhase2(this.height, this.points);
	}
	
	private LeafNode createLeafNode(ArrayList<Point> points, int start, int end, Range r, ArrayList<Range> xranges, ArrayList<Range> yranges) {
		LeafNode ln = new LeafNode(r, xranges, yranges);
		if( null == leaves) leaves = new ArrayList<LeafNode>();
		
		leaves.add(ln);
		
		ln.leafID = leaves.size() - 1;
		return ln;
	}
	
	/*
	 *  PARAMETERS:
	 *  	parent - null to kick start process;  otherwise, parent node.
	 *  	splitOn - X or Y
	 *  	points - array of points
	 *  	start, end - indexes into the points array
	 *  
	 *  - Sort subset of list based on splitOn (X or Y).
	 *  - Get median
	 *  - Create node with median.  If parent is null, set root to this.
	 *  - Recurse on the left and right.  Swap the splitOn field.
	 *  
	 */
	private Node constructPhase1(int currentHeight, int splitOn, ArrayList<Point> points, int start, int end, Range r, ArrayList<Range> xranges, ArrayList<Range> yranges) {
		assert splitOn == SplitOnX || splitOn == SplitOnY;
		
		ArrayList<Range> ranges = splitOn == SplitOnX ? xranges : yranges;

		if( currentHeight >= this.height ) {
			/* need to create a leaf node */
			return createLeafNode(points, start, end, r, xranges, yranges);
		}

		/* get the point that we are splitting on */
		int medianIndex = (start+end)/2;
		Point medianPoint = points.get(medianIndex);
		InternalNode in = new InternalNode( medianIndex, medianPoint, r, splitOn);
		
		/* set the right and left ranges */
		Range leftRange = new Range();
		
		
		if( splitOn == SplitOnX ) {
			leftRange.begin = ranges.size() > 0 ? (ranges.get( ranges.size() - 1).begin) : points.get(start).x;
			leftRange.end =  points.get(medianIndex).x;
		}
		else {
			leftRange.begin = ranges.size() > 0 ? (ranges.get( ranges.size() - 1).begin) : points.get(start).y;  
			leftRange.end =  points.get(medianIndex).y;
		}

		Range rightRange = new Range();
		
		if( splitOn == SplitOnX ) {
			rightRange.begin = Math.min( points.get(medianIndex+1).x, points.get(medianIndex).x + 1);
			rightRange.end =  ranges.size() > 0 ? ranges.get(ranges.size()-1).end : points.get(end).x;
		}
		else {
			rightRange.begin = Math.min( points.get(medianIndex+1).y, points.get(medianIndex).y + 1);
			rightRange.end =  ranges.size() > 0 ? ranges.get(ranges.size()-1).end : points.get(end).y;
		}

		int nextSplitOn = splitOn == SplitOnX ? SplitOnY : SplitOnX;		
		
		/* left child */
		/* sort on the "next sort by"  - this allows for toggling */
		Util.log( Util.Verbose, "left children- before sorting elements %d %d on %s\n", start, medianIndex, nextSplitOn == SplitOnX ? "x" : "y" );
		Util.log( Util.Verbose, "%s", points.subList(start,  medianIndex+1));
		
		Collections.sort(points.subList(start,  medianIndex+1), nextSplitOn == SplitOnX ? compareByX : compareByY );

		Util.log( Util.Verbose, "left children- after sorting on %s\n", nextSplitOn == SplitOnX ? "x" : "y" );
		Util.log( Util.Verbose, "%s", points.subList(start,  medianIndex+1));
		
		/* insert this into the appropriate ranges */
		ranges.add( leftRange );
		in.left = constructPhase1( currentHeight + 1, nextSplitOn, points, start, medianIndex, leftRange, xranges, yranges);
		ranges.remove(ranges.size()-1);
		
		/* remember the leaf ids from the left child */
		in.leafIDs.begin = in.left.getLeafIDs().begin; 

		/* right child */
		/* sort on the "next sort by"  - this allows for toggling */ 
		Util.log( Util.Verbose, "right children - before sorting elements %d %d on %s\n", medianIndex + 1, end, nextSplitOn == SplitOnX ? "x" : "y" );
		Util.log( Util.Verbose, "%s", points.subList(medianIndex+1, end+1));
		
		Collections.sort(points.subList(medianIndex + 1,  end + 1), nextSplitOn == SplitOnX ? compareByX : compareByY );

		Util.log( Util.Verbose, "right children- after sorting on %s\n", nextSplitOn == SplitOnX ? "x" : "y" );
		Util.log( Util.Verbose, "%s", points.subList(medianIndex+1, end+1));

		ranges.add( rightRange );
		in.right = constructPhase1( currentHeight + 1, nextSplitOn, points, medianIndex+1, end, rightRange, xranges, yranges);
		ranges.remove(ranges.size()-1);

		/* remember the leaf ids from the left child */
		in.leafIDs.end = in.right.getLeafIDs().end; 
		
		return in;
	}

	/* we need two static comparators - one for SplitOnX and another for SplitOnY */
	private static final Comparator<Point> compareByX = new Comparator<Point> () {
			public int compare(Point p1, Point p2) {
				return ((Integer)(p1.x)).compareTo(p2.x);
			}
	};
	private static final Comparator<Point> compareByY = new Comparator<Point> () {
		public int compare(Point p1, Point p2) {
			return ((Integer)(p1.y)).compareTo(p2.y);
		}
	};
	
	/*
	 * 
	 * We allocate sections evenly.
	 */
	void constructPhase2(int height, ArrayList <Point> points ) {
		/* move the array of points to the records */
		theRecords = new ArrayList<Record>();
		for( Point p : points ) { 
			Record record = new Record(p, -1, -1);
			theRecords.add(record);
		}
		
		/* Now allocate sections uniformly */
		int numSections = height;
		int [] sections = new int [theRecords.size()];
		for( int i = 0; i < sections.length; i++ ) {
			sections[i] = i % height;
		}
		
		int count = 0;
		for( Record record : theRecords ) {
			/* is this OK? */
			while( true ) {
				int index = (int)(Math.random() * theRecords.size());
				if( sections[index] != -1) {
					record.section = sections[index];
					sections[index] = -1;
					break;
				}
			}
		}
		
		/* now assign the leaf ids */
		assignLeafIDs(theRecords);
		
		/* print the records */
		for( Record record : theRecords ) {
			Util.log( Util.Verbose, "(%2d,%2d)", record.p.x, record.p.y );
		}
		Util.log( Util.Verbose, "\n" );
		for( Record record : theRecords ) {
			Util.log( Util.Verbose, "%7d", record.section );
		}
		Util.log( Util.Verbose, "\n" );
		for( Record record : theRecords ) {
			Util.log( Util.Verbose, "%7d", record.leafID );
		}
		Util.log( Util.Verbose, "\n" );
		
	}
	
	/* 
	 * we go through each record and look at the section.
	 * we do "section" comparisons in the tree.
	 * we stop at the node and randomly pick a leaf id under that node.
	 */
	void assignLeafIDs(ArrayList<Record> records) {
		for( Record record : records ) {
			assignLeafID(this.root, 0, record);
		}
	}

	/*
	 * walking the tree to do comparisons.
	 * 
	 * we do "section" comparisons - note that our section starts with 0 (unlike the paper)
	 * the difference between the 2d and this one is that the comparison changes from X and Y
	 */
	void assignLeafID(Node node, int counter, Record record) {
		if( counter >= record.section ) {
			/* done - choose among the leaf ids */
			record.leafID = node.getLeafIDs().begin + 
								(int)(Math.random() * (node.getLeafIDs().end - node.getLeafIDs().begin + 1));
			
			/* let the leafSection know about this also */
			this.leaves.get(record.leafID).addRecord(record.section, record.p);
		} else {
			assert node instanceof InternalNode;
			InternalNode in = (InternalNode)node;
			/* decide which way to go */
			counter++;
			if( in.splitOn == SplitOnX ) {
				Node next = record.p.x <= in.p.x ? in.left : in.right;
				Util.log( Util.Verbose, "X record [(%d) %d] Node [(%d) %d] Section [%d] Counter [%d] Choosing [%s]\n",
									record.p.x, record.p.y, in.p.x, in.p.y, record.section, counter, next == in.left ? "left" : "right" );
				assignLeafID( next, counter, record); 
			} else {
				assignLeafID( record.p.y <= in.p.y ? in.left : in.right, counter, record); 
			}
		}
	}
	
	/*
	 * generates the DOT diagram so we can view it as a tree through graphviz
	 */
	public void doDot(PrintWriter pw) {
		pw.println( "digraph graphname{");
		pw.format( "ratio=\"fill\";margin=0;\n");
		
		if( root != null ) {
			pw.format( "Node0 [shape=diamond label=\"Start\"]\n");
			pw.format( "Node0 -> Node1 [label=\"x %s\"]\n", root.getRangeAsString() );
			doDot(pw, root, 1);
		}
	
		
		pw.println( "}");
	}
	
	private int doDot(PrintWriter pw, Node n, int nodeNumber) {
		int newNodeNumber = -1;
		int thisNodeNumber = nodeNumber;
		
		assert(n != null);
		n.doDot(pw, thisNodeNumber);
		
		if( n instanceof InternalNode ) {
			InternalNode in = (InternalNode)n;
			if( in.left != null ) {
				/* print the range of the child node */
				pw.format("Node%d -> Node%d [label=\"%s %s\"]\n", thisNodeNumber, nodeNumber+1, in.splitOn == SplitOnX ? "x" : "y", in.left.getRangeAsString() );
				newNodeNumber = doDot(pw, in.left, nodeNumber+1);
				if( newNodeNumber > nodeNumber ) nodeNumber = newNodeNumber;
			}
			if( in.right != null ) {
				pw.format("Node%d -> Node%d [label=\"%s %s\"]\n", thisNodeNumber, nodeNumber+1, in.splitOn == SplitOnX ? "x" : "y", in.right.getRangeAsString() );
				newNodeNumber = doDot(pw, in.right, nodeNumber+1);
				if( newNodeNumber > nodeNumber ) nodeNumber = newNodeNumber;
			}
		}
		
		return nodeNumber > newNodeNumber ? nodeNumber : newNodeNumber;
	}
	

	private static int testCounter = 0;
	
	public static void main(String args[] ) {
		// test1();
		testMany();
	}
	
	private static void test1() {
		/* data sets */
		int x[] = 			{10, 30, 50, 55, 49, 12, 35, 85, 50, 32, 45, 55, 12, 6,  8,  35, 45, 52, 95, 82};
		int y[] = 			{80, 45, 54, 66, 12, 34, 49, 88, 22, 32, 12, 2,  60, 12, 15, 20, 22, 80, 50, 32};
		Rect query = new Rect(0,0,60,60);
		int expected[] = 	{ 0,  1,  1,  0,  1,  1,  1,  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  0,  0,  0};
		
		testResults(x, y, expected, query);
	}
	
	private static boolean testResults(int x[], int y[], int expected[], Rect query) {

		boolean ret = false;
		
		/* Save the X, Y, results, and query in the file */
		String testData = String.format( "data/data%d", testCounter++ ); 
		try {
			PrintWriter pwTestData = new PrintWriter( testData );
			pwTestData.printf( "X[] = {" );
			for( int i = 0; i < x.length; i++ ) 
				pwTestData.printf( "%3d%s", x[i], i == x.length -1 ? " " : "," );
			pwTestData.printf( "}\nY[] = {" );
			for( int i = 0; i < y.length; i++) 
				pwTestData.printf( "%3d%s", y[i], i == y.length -1 ? " " : "," );
			pwTestData.printf( "}\nE[] = " );
			for( int i = 0; i < expected.length; i++)
				pwTestData.printf( "%3d%s", expected[i], i == expected.length -1 ? " " : "," );
			pwTestData.printf( "\nRect = %s\n", query);
			
		
			/* construct the tree */
			ACETree2d tree = constructTree(createPointsArray(x,y), testData);
			
			if( tree != null ) {
				/* a little wasteful, but our tree sorts the points so we cannot use it for validation :-( */
				ret = checkResults(createPointsArray(x,y), tree, query, expected, pwTestData);
			}
				
			pwTestData.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return ret;
	}
	
	private static ArrayList<Point> createPointsArray(int x[], int y[]) {
		/* create an array of points */
		ArrayList<Point> points = new ArrayList<Point>();

		assert x.length == y.length;
		for( int i = 0; i < x.length; i++ ) {
			points.add( new Point(x[i], y[i] ));
		}
		
		return points;
	}
	
	private static ACETree2d constructTree(ArrayList<Point> points, String testData) {
		if( points.size() == 0 ) {
			Util.log( Util.None, "constructTree called with zero length Points Array\n" );
			return null;
		}
		
		/* draw the points first */
		// drawPoints(testData, points);

		/* 
		 * 	construct the tree
		 * 
		 * 	we need to run the following command to generate a PNG file:
		 * 
		 * 	dot -Tpng -O ace2d.dot
		 * 
		 *  It may be a good idea to run this automatically.
		 * 
		 */
		ACETree2d tree = new ACETree2d( points );
		String sDotFile = String.format("%s.dot",  testData);
		
		try {
			PrintWriter pw = new PrintWriter(sDotFile);
			tree.doDot(pw);
			pw.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
				
		return tree;
	}
	
	private static boolean checkResults(ArrayList<Point> points, ACETree2d tree, Rect query, int expected[], PrintWriter pwTestData) {
		Util.log( Util.Minimal, "points size %d expected size %d\n", points.size(), expected.length);

		assert points.size() == expected.length;
		
		ACETree2dSearch treeSearch = new ACETree2dSearch(tree, query);
		
		ArrayList<Point> result = new ArrayList<Point>();
		
		while( !treeSearch.done() ) {
			ArrayList<Point> r1 = treeSearch.search();
			if( r1 != null )
				result.addAll(r1);
		}
		
		pwTestData.printf( "Result: %s\n", result );
		
		/* check the result now */
		int index = 0;
		int errCount = 0;
		int falsepositive = 0;
		int truenegative = 0;
		for( Point p : points ) {
			if( result.contains(p) ) {
				if( expected[index] != 1) {
					Util.log( Util.Minimal, "no match in expected array - index [%d] Point [%s]\n", index, p);
					errCount++;
					falsepositive++;
				}
			}
			else {
				if( expected[index] != 0) {
					Util.log( Util.Minimal, "no match in expected array - index [%d] Point [%s]\n", index, p);
					errCount++;
					truenegative++;
				}
			}
			
			index++;
		}
		
		Util.log( Util.None, "there are %d errors %d false positive and %d true negative \n", errCount, falsepositive, truenegative);
		pwTestData.printf( "there are %d errors %d false positive and %d true negative \n", errCount, falsepositive, truenegative);
		
		return errCount > 0 ? false : true;
	}
	
	/*
	private static void drawPoints( String testData, ArrayList<Point> points) {
        SVGGraphics2D g = new SVGGraphics2D(0.0, 0.0, 400, 400);
        Drawing d = new Drawing(g);
        d.drawAxes();
		d.drawPoints( points );
		d.save( String.format( "%s.svg",  testData ) );
	}
	*/
	
	protected static final int MaxXYValue = 100;
	protected static final int MaxNumPoints = 100;
	protected static final int MaxNumTests = 10;
	
	private static void testMany() {
		
		int numSucceeded = 0;
		for( int numTests = 0; numTests < ACETree2d.MaxNumTests; numTests++ ) {
			Util.log( Util.None, "Performing Test Iteration #%d\n",  numTests );
			
			/* minimum of 4 points -> we can revisit this later */
			int total = 0;
			while( total <= 4 ) {
				total = (int)(Math.random() * ACETree2d.MaxNumPoints);
			}
			
			int x[] = new int [total];
			int y[] = new int [total];

			for( int num = 0; num < total; num++ ) {
				/* generate X and Y values */
				x[num] = (int)(Math.random() * MaxXYValue);
				y[num] = (int)(Math.random() * MaxXYValue);
			}
				
			/* now generate rectangle */
			int left = (int)(Math.random() * MaxXYValue/2);
			int bottom = (int)(Math.random() * MaxXYValue/2);
			int right = 0, top = 0;
			while( right <= left ) {
				right = (int)(Math.random() * MaxXYValue);
			}
			while( top <= bottom ) {
				top = (int)(Math.random() * MaxXYValue);
			}
			Rect query = new Rect(left, bottom, right, top);
				
			int expected[] = generateExpected(x,y,query);
				
			numSucceeded += testResults(x, y, expected, query) ? 1 : 0;
		}
		
		Util.log( Util.None, "***  Number of tests [%d] Number of Successes [%d]\n", ACETree2d.MaxNumTests, numSucceeded );
	}
	
	private static int [] generateExpected(int x[], int y[], Rect rect) {
		int results[] = new int [ x.length ];
		
		for( int i = 0; i < x.length; i++ ) {
			Point p = new Point( x[i], y[i] );
			results[i] = rect.includes(p) ? 1 : 0;
		}
		
		return results;
	}
}


