import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.log4j.Logger;

public class BoggleMapper extends Mapper<LongWritable, Text, Text, RollGraphWritable> {
	private static final Logger logger = Logger.getLogger(BoggleMapper.class);

	private BoggleRoll roll;
	
	private BloomFilter bloomFilter;

	@Override
	public void setup(Context context) throws IOException {
		Configuration configuration = context.getConfiguration();
		
		roll = BoggleRoll.deserialize(configuration.get("roll"));
		
		FileSystem fileSystem = FileSystem.get(configuration);
		
		bloomFilter = new BloomFilter(UserDictBloom.vectorSize, UserDictBloom.nbHash, UserDictBloom.hashType);		
		bloomFilter.readFields(fileSystem.open(new Path(configuration.get("bloompath"))));
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Expected input:
		// aaaaa [[0,0][1,1][2,2]] false
		String line = value.toString();

		String values[] = line.split("\\s");

		if (values.length == 3) {
			String charsSoFar = values[0];
			
			RollGraphWritable rollGraph = RollGraphWritable.deserialize(values[1] + " " + values[2]);

			if (!rollGraph.isFinal) {
				// Mark node as exhausted and emit
				rollGraph.isFinal = true;
				context.write(new Text(charsSoFar), rollGraph);

				// Emit the letters around it
				Node node = rollGraph.nodes.get(rollGraph.nodes.size() - 1);

				for (int row = node.row - 1; row < node.row + 1; row++) {
					if (row < 0 || row >= BoggleRoll.letters.length) {
						// Check if row is outside the bounds and skip if so
						continue;
					}

					for (int col = node.column - 1; col < node.column + 1; col++) {
						if (col < 0 || col >= BoggleRoll.letters.length) {
							// Check if column is outside the bounds and skip if so
							continue;
						}

						// Found viable row and column. See if node has already been traversed
						Node nextNode = new Node(row, col);

						if (!rollGraph.nodes.contains(nextNode)) {
							// Node not found, see if it passes the membership test
							String newWord = charsSoFar + roll.rollCharacters[row][col];
							
							if (bloomFilter.membershipTest(new Key(newWord.getBytes()))) {
								// It might exist, create new object, add new node, and emit
								@SuppressWarnings("unchecked")
								ArrayList<Node> nextNodeList = (ArrayList<Node>) rollGraph.nodes.clone();
								nextNodeList.add(nextNode);
								
								RollGraphWritable nextGraphWritable = new RollGraphWritable(nextNodeList, false);
								
								context.write(new Text(newWord), nextGraphWritable);
							}
						}
					}
				}
			} else {
				context.write(new Text(charsSoFar), rollGraph);
			}
		} else {
			logger.warn("The input line had more spaces than were expected.  Had " + values.length
					+ " expected 3.  The line was \"" + line + "\"");
		}
	}
}
