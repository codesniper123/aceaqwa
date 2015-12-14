package tools;

import java.util.ArrayList;
import java.util.HashMap;

import tools.ACETree2d.InternalNode;
import tools.ACETree2d.LeafNode;
import tools.ACETree2d.LeafSection;
import tools.ACETree2d.Node;

public class ACETree2dSearch {
	/* 
	 * searching data structures:
	 * 
	 * We keep track of the following for each node on the tree.
	 */
	class NodeSearchInfo {
		private static final int LEFT = 1;
		private static final int RIGHT = 2;
		private int next = LEFT;;
		private boolean done = false;
		
		public NodeSearchInfo() {
			next = LEFT;
			done = false;
		}
	};
	
	/* tbd: do we need to come up with a good Hash function for the Node class? */
	private HashMap<Node, NodeSearchInfo> searchNodes;
	
	private ACETree2d tree;
	
	/* this needs to change to a Rect */
	private Rect queryRect;
	
	/*
	 * - Adding sections is a little involved.
	 * - We can add only sections where the previous dimensions match.  Thus, there is a single "partner" node for each node
	 * - The number of sections in each leaf is equal to the height of the tree.
	 * 
	 * - We define a data structure called BucketEntry that has the following:
	 *    - section
	 *    - leaf id range
	 *    - sorted by
	 *    - data range
	 *    - actual points to be stored for aggregation
	 *    - flag (set/unset/invalid) - set -> data is present;  unset -> expecting data;  invalid -> not eligible for the current query
	 *    
	 * - We need a data structure that would allow us to locate the right BucketEntry given the section number and the leaf id.
	 *  Let us take an example:  Let us take height = 4 and hence the number of leaf nodes = 2^(height-1) = 2^3 = 8.
	 *  The number of sections in each leaf node = 4. (same as height).
	 *  Section 0 will have the complete range of X.  Thus, there is only one BucketEntry for this.
	 *  Section 1 will have 2 ranges of X.  Hence we will have 2 BucketEntries.  Note that these are "partners" i.e they complete the full range.
	 *  Things get interesting after this.  Since we split on Y after this, we cannot really combine all the 4 entries for Section = 2.
	 *  So Section 2 will have 4 entries for Y, but they combine to make 2 different ranges.  The first 2 make partners and the next two make partners as well.
	 *  Section 3 will have 8 Bucket Entries with 4 sets of partners.
	 *  
	 *  The BucketEntries can be arranged in an array:
	 *  
	 *  Index  Section Leaf Range  
	 *  0      0       0-7			no partner
	 *  1      1       0-3			partners with BucketEntry with index 2
	 *  2      1       4-7			partners with BucketEntry with index 1
	 *  3      2       0-1
	 *  4      2       2-3
	 *  5      2       4-5
	 *  6      2       6-7
	 *  7      3       0  
	 *  8      3       1  
	 *  9      3       2  
	 *  10     3       3  
	 *  11     3       4  
	 *  12     3       5  
	 *  13     3       6  
	 *  14     3       7   
	 *  	 *  
	 *  Some calculations:
	 *  
	 *  1) The size of this array = 2^height - 1
	 *  2) Number of leaf nodes for a given section = 2^(height-section-1)
	 *     For example, for section = 0, we have 2^(4-0-1) = 8 leaf ids (0 through 7)
	 *  3) Calculating the array index given the section and leaf id:
	 *     2^section + leafid / (2^(height-section-1)
	 *     for example, if leaf id = 3 and section = 3, we have:
	 *     (2^3 - 1) + 3 / 2^(4-3-1)
	 *     7 + 3 = 10
	 *  4) In order to find the partner:
	 *     if array index is Odd, the partner is the next element in the array
	 *     if array index is Even, the partner is the previous element (except when the index = 0 in which case there is no partner)
	 * 
	 */
	
	class Bucket {
		protected static final int BucketInvalid = 1;
		protected static final int BucketSet = 2;
		protected static final int BucketUnset = 3;
 		
		protected int section;
		protected Range leafIDRange;
		protected int sortBy;
		protected Range dataRange;
		protected ArrayList<Point> points;
		protected int flag;
		
		public Bucket(int section, Range leafIDRange, int sortBy, Range dataRange, Rect queryRect) {
			this.section = section;
			this.leafIDRange = leafIDRange;
			this.sortBy = sortBy;
			this.dataRange = dataRange;
			this.points = new ArrayList<Point>();
			
			/* if the data range does not overlap the queryRect, set this bucket to Invalid otherwise it is just unset. */
			boolean overlaps = dataRange.overlaps( sortBy == ACETree2d.SplitOnX ? queryRect.xrange : queryRect.yrange );
			this.flag = overlaps ? BucketUnset : BucketInvalid;  		// we can try to set it here itself...
		}
		
		void reinit() {
			if( this.flag != Bucket.BucketInvalid )
				this.flag = Bucket.BucketUnset;
			
			this.points = new ArrayList<Point>();
		}
		
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append( String.format( "section=[%d] leafIDRange: [%d-%d], sortBy [%s] dataRange [%d-%d]", 
								section, leafIDRange.begin, leafIDRange.end, ACETree2d.getSplitOnStr(sortBy), dataRange.begin, dataRange.end ) );
			int i = 0;
			for( Point p : points ) {
				sb.append (String.format("%s %s", i == 0 ? " Points: " : " ", p));
				i++;
			}
			
			return sb.toString();
			
		}
		
		/*
		 * adds points to this bucket.
		 * 
		 * "sets" the flag.  note that elements can be added to an already "set" bucket.
		 */
		boolean addLeafSection(LeafSection section, Rect queryRect) {
			/* check if we overlap with this rectangle */
			assert this.dataRange.overlaps( this.sortBy == ACETree2d.SplitOnX ? queryRect.xrange : queryRect.yrange);
			assert this.flag != Bucket.BucketInvalid;
			
			/* copy the points from section to us */
			for( Point p : section.points )
				this.points.add(p);
			
			this.flag = Bucket.BucketSet;
			
			return true;
		}
	}
	
	
	protected Bucket buckets[];
	
	private Bucket getPartnerBucket(int height, Bucket one) {
		/* 
		 * If bucket id is zero, return null 
		 * If bucket id is even, return bucket id - 1
		 * If bucket id is odd, return bucket id + 1 
		 */
		
		/* get bucket id */
		int bucketIndex = getBucketIndex(height, one.section, one.leafIDRange);
		assert bucketIndex >= 0 && bucketIndex < buckets.length;
		
		if( bucketIndex == 0 ) {
			return null;
		} else if( (bucketIndex & 0x01) == 1 ) {
			/* odd */
			return buckets[bucketIndex + 1];
		} else {
			return buckets[bucketIndex - 1];
		}
	}
	
	/*
	 * Given one bucket, see if the partner is completed;  if so, process both of them.
	 */
	private boolean checkAndProcessBuckets(int height, Rect queryRect, Bucket oneOfThem, ArrayList<Point> result) {
		assert oneOfThem.flag != Bucket.BucketInvalid;
		if( oneOfThem.flag == Bucket.BucketUnset )
			return false;
		
		/* get partner */
		Bucket partnerBucket = getPartnerBucket(height, oneOfThem);
		
		/* 
		 * The "partnerBucket == null can occur if the queryRect has a x coordinate that encapsulates the widest "x" we have.
		 */
		if( oneOfThem.flag == Bucket.BucketSet && (partnerBucket == null || partnerBucket.flag == Bucket.BucketSet || partnerBucket.flag == Bucket.BucketInvalid) ){
			Util.log( Util.Verbose, "processing bucket 1 (section %d, leafid range %s) and bucket 2 (section %d leafid %s\n",
							oneOfThem.section, oneOfThem.leafIDRange, 
							partnerBucket != null ? partnerBucket.section : -1, 
							partnerBucket != null ? partnerBucket.leafIDRange : "(empty)" ); 
			
			/* process both */
			ACETree2dSearch.filterAndAdd(queryRect,  oneOfThem.points, result);
			if( partnerBucket != null && partnerBucket.flag != Bucket.BucketInvalid )
				ACETree2dSearch.filterAndAdd(queryRect,  partnerBucket.points, result);
			
			oneOfThem.reinit();
			if( partnerBucket != null )
				partnerBucket.reinit();
			
			return true;
		}
		
		return false;
	}
	
	void dumpBuckets() {
		int i = 0; 
		for( Bucket bucket : buckets )  {
			Util.log(Util.Verbose, "[%d] - %s\n", i, bucket );
			i++;
		}
	}
	
	public ACETree2dSearch(ACETree2d aceTree, Rect queryRect) {
		this.tree = aceTree;
		this.queryRect= queryRect;

		/* initialize the search nodes */
		this.searchNodes = new HashMap<Node, NodeSearchInfo> ();
		initSearchNodes2(tree.root, this.searchNodes);
		
		/* initialize the buckets */
		initBuckets(this.tree.height, queryRect);
		
		int bucketID = 0;
		for( Bucket bucket : buckets ) {
			Util.log(Util.Verbose, "[%d]: %s\n", bucketID, bucket );
			bucketID++;
		}
	}

	/*
	 * The main interface to do the search.
	 * 
	 * You can call this repeatedly while checking for done().
	 */
	public ArrayList<Point> search() {
		Util.log(Util.Minimal,"Searching with %s\n", this.queryRect );
		
		ArrayList<Point> result = new ArrayList<Point>();
		shuttle(tree.height, tree.root, this.queryRect, result);
		return result;
	}
	
	/*
	 * Terminating condition for the search 
	 */
	public boolean done() { 
		return isSearchDone(this.tree.root);
	}
	
	
	/*
	 * initializing search data structures 
	 */
	private void initSearchNodes2(Node node, HashMap<Node, NodeSearchInfo> searchNodes) {
		/* end condition */
		if( node == null ) 
			return;
		
		/* recurse */
		this.searchNodes.put(node,  new NodeSearchInfo());
		if( node instanceof InternalNode ) {
			InternalNode in = (InternalNode)node;
			
			initSearchNodes2(in.left, searchNodes);
			initSearchNodes2(in.right, searchNodes);
		} 
	}
	
	/*
	 * This sets up the Buckets from the given tree and query.
	 * 
	 * 1.  Allocate the buckets with the correct # of entries.  We will use the Java array since we just need a fixed size array.
	 * 2.  Add section 0 entry (since it is special)
	 * 3.  Call recursive function.  At each node, we need to add the children as entries.
	 * 			- section is incremented.
	 * 			- we get the leafids, data range from the children.
	 * 			- we get the sortby from the node
	 * 
	 */
	private void initBuckets(int height, Rect queryRect) {
		/* let us get the size which is 2^(height) - 1 */
		int numBuckets = Util.twoPowerN(height) - 1;
		this.buckets = new Bucket[numBuckets];
		
		/* add for root */
		Bucket bucket = new Bucket(0, this.tree.root.getLeafIDs(), ACETree2d.SplitOnX, this.tree.root.getValueRange(), queryRect);
		this.buckets[0] = bucket;
		
		initBuckets2(queryRect, height, 1, this.tree.root);
	}
	
	private void initBuckets2(Rect queryRect, int height, int section, Node node) {
		if( node instanceof LeafNode ) 
			return;
		
		assert node instanceof InternalNode;
		InternalNode in = (InternalNode)node;

		Bucket bucket = new Bucket(section, in.left.getLeafIDs(), in.splitOn, in.left.getValueRange(), queryRect);
		int bucketIndex = getBucketIndex(height, section, bucket.leafIDRange);
		assert bucketIndex > 0 && bucketIndex < this.buckets.length;
		this.buckets[bucketIndex] = bucket;
		
		bucket = new Bucket(section, in.right.getLeafIDs(), in.splitOn, in.right.getValueRange(), queryRect);
		bucketIndex = getBucketIndex(height, section, bucket.leafIDRange);
		assert bucketIndex > 0 && bucketIndex < this.buckets.length;
		this.buckets[bucketIndex] = bucket;
		
		/* recurse */
		initBuckets2(queryRect, height, section+1, in.left);
		initBuckets2(queryRect, height, section+1, in.right);
	}

	/*
	 *  Formula:
	 *           A              B                 C
	 *     (2^section - 1) + (leafid) / (2^(height-section-1)
	 *     
	 *   "A" just gives us the number of entries so far i.e. for all sections so far.
	 *   "B" and "C" account for the fact that there are "more" nodes as you go down the tree.
	 *       Thus, if section = 0, then all the leaf ids belong to this node.
	 *       Whereas if section = height-1, then there is one leaf id per node.
	 *       Note that section runs from an index of 0 to height - 1.
	 *  
	 */
	protected int getBucketIndex(int height, int section, Range leafIDRange) {
		int start = Util.twoPowerN(section) - 1 + ( leafIDRange.begin / Util.twoPowerN(height-section-1) );   
		int end = Util.twoPowerN(section) - 1 + ( leafIDRange.begin / Util.twoPowerN(height-section-1) );
		return start == end ? start : -1;
	}
	
	
	private boolean isSearchDone(Node in) {
		NodeSearchInfo searchInfo = this.searchNodes.get(in);
		assert searchInfo != null;
		return searchInfo.done;
	}
	
	private void setSearchDone(Node in ) {
		NodeSearchInfo searchInfo = this.searchNodes.get(in);
		assert searchInfo != null;
		searchInfo.done = true;
		
	}
	
	
	/*
	 * checks if a particular node overlaps with the query range.
	 * 
	 * checks for the splitOn and does the X or Y comparison.
	 */
	private boolean overlaps(int splitOn, Rect queryRect, Node node) {
		return splitOn == ACETree2d.SplitOnX ?  
					queryRect.xrange.overlaps(node.getValueRange()) : 
					queryRect.yrange.overlaps(node.getValueRange());
	}
	
	/*
	 * The "shuttle" name is borrowed from the Algorithm described in the paper.
	 * 
	 * Please see algorithm described - we have tried to be true to it.
	 * 
	 * PARAMETERS:
	 * node: 		starts with the root and then visits all the nodes.
	 * queryRect: 	rectangle for the range query
	 * result:		points returned to the caller.
	 */
	private void shuttle(int height, Node node, Rect queryRect, ArrayList<Point> result) {
		Util.log(Util.Verbose, "shuttle with " + node);
		
		if( node instanceof LeafNode ) {
			combineTuples(height, node, queryRect, result);
			setSearchDone(node);
		}
		else {
			InternalNode in = (InternalNode)node;
			if( isSearchDone( in.left ) && isSearchDone( in.right ) ) {
				setSearchDone(node);
			} else if( !isSearchDone(in.right) && isSearchDone(in.left) ) {
				/* only right is not done */
				shuttle(height, in.right, queryRect, result);
			} else if( !isSearchDone(in.left) && isSearchDone(in.right) ) {
				/* only left is not done */
				shuttle(height, in.left, queryRect, result);
			} else {
				/* both the children are not done: */
				if( this.overlaps(in.splitOn, queryRect, in.left) && !this.overlaps(in.splitOn, queryRect, in.right)) {
					/* overlaps only with left */
					shuttle(height, in.left, queryRect, result);
				} else if( this.overlaps(in.splitOn, queryRect, in.right) && !this.overlaps(in.splitOn, queryRect, in.left)) {
					/* overlaps only with right */
					shuttle(height, in.right, queryRect, result);
				} else {
					/* overlaps both sides or none */
					NodeSearchInfo searchInfo = this.searchNodes.get(node);
					if( searchInfo.next == NodeSearchInfo.LEFT ) {
						shuttle(height, in.left, queryRect, result);
						searchInfo.next = NodeSearchInfo.RIGHT;
					}
					else if( searchInfo.next == NodeSearchInfo.RIGHT ) {
						shuttle(height, in.right, queryRect, result);
						searchInfo.next = NodeSearchInfo.LEFT;
					}
				}
			}
		}
	}
	

	/* 
	 * for each section in the leaf:
	 * 		- if section encapsulates the query range completely, process section and do not store it.
	 * 		- if section does not overlap the query range at all, ignore it.
	 * 		- if section overlaps the query range, install it in the buckets.
	 * 			- if the buckets are complete i.e. the other partner is already present or INVALID, process bucket.
	 * 			- if bucket is incomplete, store the elements in this section into the bucket.
	 */
	private void combineTuples(int height, Node n, Rect queryRect, ArrayList<Point> result) {
		assert n instanceof LeafNode;
		LeafNode leaf = (LeafNode)n;
		
		Util.log(Util.Verbose,"combineTuples: leafNode index %d\n", leaf.leafID);
		
		int section = 0;
		for( ACETree2d.LeafSection leafSection : leaf.leafSections ) {			
			Util.log(Util.Verbose,"combineTuples - section %d leafSection.r %s queryRect %s\n",  section, leafSection.r, queryRect);

			if( leafSection.r.encapsulates( leafSection.splitOn == ACETree2d.SplitOnX ? queryRect.xrange : queryRect.yrange) ) {
				Util.log(Util.Verbose,"section %d encapuslated - going to filter and add\n", section);
				filterAndAdd(queryRect, leafSection.points, result);
			} else if( leafSection.r.overlaps(leafSection.splitOn == ACETree2d.SplitOnX ? queryRect.xrange : queryRect.yrange)) {
				Util.log(Util.Verbose,"section %d overlaps - some more processing\n", section);
				
				/* get the right bucket for this */
				int bucketID = getBucketIndex(height, section, leaf.getLeafIDs());
				assert bucketID >= 0 && bucketID < buckets.length;
				Bucket bucket = buckets[bucketID];
				
				bucket.addLeafSection(leafSection, queryRect);
				
				checkAndProcessBuckets(height, queryRect, bucket, result);				
			} else {
				/* ignore section  - we could ascertain that the corresponding bucket is INVALID */
				Util.log(Util.Verbose,"Ignoring section %d\n",  section);
			}
			section++;
		}
		
		/* let us dump the buckets to indicate what we have */
		dumpBuckets();
	}
	
	private static void filterAndAdd(Rect queryRect, ArrayList<Point> src, ArrayList<Point> dest) {
		boolean first = true;
		for( Point pt : src ) {
			if( first ) Util.log(Util.Verbose,"Filter and add " );
			Util.log(Util.Verbose, "%s ", pt );
			if( queryRect.includes(pt) ) {
				Util.log(Util.Verbose, " (added) ");
				dest.add(pt);
			} else {
				Util.log(Util.Verbose, " (not added) ");
			}
			first = false;
		}
		Util.log(Util.Verbose, "\n");
	}
}


