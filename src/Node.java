public class Node {
	public int row, column;

	public Node(int row, int column) {
		this.row = row;
		this.column = column;
	}

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