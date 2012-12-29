import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;

/**
 * A custom writable that represents the nodes traversed in a Boggle roll graph while trying to find a word.
 * 
 * @author jesseanderson
 * 
 */
public class RollGraphWritable implements Writable {
	/** The nodes traversed so far */
	ArrayList<Node> nodes = new ArrayList<Node>();

	/** If the node's children have been traversed */
	boolean isFinal;

	/**
	 * Constructor
	 * 
	 * @param nodes
	 *            List of nodes traversed so far
	 * @param isFinal
	 *            If the node's children have been traversed
	 */
	public RollGraphWritable(ArrayList<Node> nodes, boolean isFinal) {
		this.nodes = nodes;
		this.isFinal = isFinal;
	}

	/**
	 * Empty constructor for serialization
	 */
	public RollGraphWritable() {
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isFinal);

		// Write out the number of nodes
		out.writeInt(nodes.size());

		// Write out the nodes
		for (Node node : nodes) {
			out.writeShort(node.row);
			out.writeShort(node.column);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodes.clear();

		isFinal = in.readBoolean();

		// See how many nodes have been traversed
		int numNodes = in.readInt();

		// Read in the nodes
		for (int i = 0; i < numNodes; i++) {
			short row = in.readShort();
			short column = in.readShort();

			nodes.add(new Node(row, column));
		}
	}

	/**
	 * Serializes the object to a string representation
	 * 
	 * @return The string representation
	 */
	public String serialize() {
		return toString();
	}

	/**
	 * Deserializes the string to a RollGraphWritable
	 * 
	 * @param rollString
	 *            The string represenation
	 * @return The RollGraphWritable in the string
	 */
	public static RollGraphWritable deserialize(String rollString) {
		String[] parts = rollString.split(" ");

		// Use Regex to read out the rows and columns in the string
		Pattern pattern = Pattern.compile("\\[(\\d*),(\\d*)\\]");
		String nodesString = parts[0].substring(1, parts[0].length() - 1);
		Matcher nodeMatcher = pattern.matcher(nodesString);

		ArrayList<Node> nodes = new ArrayList<Node>();

		// Go through all groups and add their rows and columns
		while (nodeMatcher.find()) {
			Node node = new Node(nodeMatcher.group(1), nodeMatcher.group(2));
			nodes.add(node);
		}

		RollGraphWritable graphWritable = new RollGraphWritable(nodes, Boolean.parseBoolean(parts[1]));

		return graphWritable;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();

		buffer.append("[");

		for (Node node : nodes) {
			buffer.append("[").append(node.row).append(",").append(node.column).append("]");
		}

		buffer.append("] ");

		buffer.append(isFinal);

		return buffer.toString();
	}
}
