/**
 * Represents a single node in the graph.
 * 
 * @author jesseanderson
 *
 */
public class Node {
	/** The node's row in the roll */
	public int row;
	/** The node's column in the roll */
	public int column;

	/**
	 * Constructor
	 * @param row The node's row in the roll
	 * @param column The node's column in the roll
	 */
	public Node(int row, int column) {
		this.row = row;
		this.column = column;
	}

	/**
	 * Constructor
	 * @param row The node's row in the roll
	 * @param column The node's column in the roll
	 */
	public Node(String row, String column) {
		this.row = Integer.parseInt(row);
		this.column = Integer.parseInt(column);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Node) {
			Node otherNode = (Node) o;
			return row == otherNode.row && column == otherNode.column;
		} else {
			return false;
		}
	}
}