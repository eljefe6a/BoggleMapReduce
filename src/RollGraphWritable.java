import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;


public class RollGraphWritable implements Writable {
	ArrayList<Node> nodes = new ArrayList<Node>();
	
	boolean isFinal;
	
	public RollGraphWritable(ArrayList<Node> nodes, boolean isFinal) {
		this.nodes = nodes;
		this.isFinal = isFinal;
	}
	
	public RollGraphWritable() {
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isFinal);
		
		out.writeInt(nodes.size());
		
		for (Node node : nodes) {
			out.writeInt(node.row);
			out.writeInt(node.column);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodes.clear();
		
		isFinal = in.readBoolean();
		
		int numNodes = in.readInt();
		
		for (int i = 0; i < numNodes; i++) {
			int row = in.readInt();
			int column = in.readInt();
			
			nodes.add(new Node(row, column));
		}
	}
	
	public String serialize() {
		return toString();
	}
	
	public static RollGraphWritable deserialize(String rollString) {
		String[] parts = rollString.split(" ");
		
		Pattern pattern = Pattern.compile("\\[(\\d*),(\\d*)\\]");
		String nodesString = parts[0].substring(1, parts[0].length() - 1);
		Matcher nodeMatcher = pattern.matcher(nodesString);
		
		ArrayList<Node> nodes = new ArrayList<Node>();
		
		while(nodeMatcher.find()) {
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
