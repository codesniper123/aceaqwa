package tools;

public class Rect {
	/* tbd: is this a strange way to represent a rect? */
	protected Range xrange;
	protected Range yrange;
	
	public Rect(int left, int bottom, int right, int top) {
		assert left < right;
		assert bottom < top;
		xrange = new Range(left, right);
		yrange = new Range(bottom, top);
		
	}
	public Rect(Range xrange, Range yrange) {
		this.xrange = xrange;
		this.yrange = yrange;
	}
	
	public boolean includes(Point p) {
		return xrange.includes(p.x) && yrange.includes(p.y);
	}
	
	public String toString() {
		return String.format( "[x(%d,%d) y(%d,%d)]", xrange.begin, xrange.end, yrange.begin, yrange.end );
	}
}
