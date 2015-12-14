package tools;

public class Point extends Object {
	protected int x, y;
	
	public Point(int x, int y) {this.x = x; this.y = y; }
	
	@Override 
	public String toString() {
		return String.format( "(%d-%d)", x,y);
	}	
	
	@Override
	public boolean equals(Object another) {
		assert another instanceof Point;
		Point thatPoint = (Point)another;
		return this.x == thatPoint.x && this.y == thatPoint.y ? true : false;
	}
}


