import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

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
