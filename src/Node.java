/**
 * Represents a single node in the graph.
 * 
 * @author jesseanderson
 * 
 */
public class Node {
	/** The node's row in the roll */
	public short row;
	/** The node's column in the roll */
	public short column;

	/**
	 * Constructor
	 * 
	 * @param row
	 *            The node's row in the roll
	 * @param column
	 *            The node's column in the roll
	 */
	public Node(short row, short column) {
		this.row = row;
		this.column = column;
	}

	/**
	 * Constructor that converts strings
	 * 
	 * @param row
	 *            The node's row in the roll
	 * @param column
	 *            The node's column in the roll
	 */
	public Node(String row, String column) {
		this.row = Short.parseShort(row);
		this.column = Short.parseShort(column);
	}

	/**
	 * Constructor that converts ints to shorts
	 * 
	 * @param row
	 *            The node's row in the roll
	 * @param column
	 *            The node's column in the roll
	 */
	public Node(int row, int column) {
		if (row > Short.MAX_VALUE || column > Short.MAX_VALUE) {
			throw new RuntimeException("Row or column exceeds short.  Row was " + row + " column was " + column
					+ " and both should be less than " + Short.MAX_VALUE);
		}

		this.row = (short) row;
		this.column = (short) column;
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