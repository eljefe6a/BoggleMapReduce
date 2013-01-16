import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.log4j.Logger;

public class BoggleMapper extends Mapper<Text, RollGraphWritable, Text, RollGraphWritable> {
	private static final Logger logger = Logger.getLogger("Boggle");

	/** The Boggle Roll that is being process */
	private BoggleRoll roll;

	/** The Bloom Filter with the dictionary */
	private BloomFilter bloomFilter;

	@Override
	public void setup(Context context) throws IOException {
		Configuration configuration = context.getConfiguration();

		// Get the Boggle Roll
		roll = BoggleRoll.deserialize(configuration.get(BoggleDriver.ROLL_PARAM));

		// Load the Bloom Filter
		FileSystem fileSystem = FileSystem.get(configuration);

		if (configuration.getBoolean(BoggleDriver.ENABLE_BLOOM_PARAM, BoggleDriver.ENABLE_BLOOM_DEFAULT)) {
			// Only allow BloomFilter usage if it's turned on
			bloomFilter = new BloomFilter(UserDictBloom.VECTOR_SIZE, UserDictBloom.NBHASH, UserDictBloom.HASH_TYPE);
			bloomFilter.readFields(fileSystem.open(new Path(configuration.get(BoggleDriver.BLOOM_PARAM))));
		}
	}

	@Override
	public void map(Text key, RollGraphWritable value, Context context) throws IOException, InterruptedException {
		if (!value.isFinal) {
			processNonFinalNode(context, key.toString(), value);
		} else {
			context.write(key, value);

			// Use counters to keep track of how many words were found so far
			context.getCounter("boggle", "words").increment(1);
		}
	}

	/**
	 * Emits the nodes around the last processed node
	 * 
	 * @param context
	 *            The context object for incrementing
	 * @param charsSoFar
	 *            The characters making up the node so far
	 * @param rollGraph
	 *            The RollGraphWritable representing the nodes
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void processNonFinalNode(Context context, String charsSoFar, RollGraphWritable rollGraph)
			throws IOException, InterruptedException {
		// Mark node as exhausted and emit
		rollGraph.isFinal = true;
		context.write(new Text(charsSoFar), rollGraph);

		// Emit the characters around the last node in the Boggle Roll
		Node node = rollGraph.nodes.get(rollGraph.nodes.size() - 1);

		boolean proceed;
		
		for (int row = node.row - 1; row < node.row + 2; row++) {
			if (row < 0 || row >= roll.rollSize) {
				// Check if row is outside the bounds and skip if so
				continue;
			}

			for (int col = node.column - 1; col < node.column + 2; col++) {
				if (col < 0 || col >= roll.rollSize) {
					// Check if column is outside the bounds and skip if so
					continue;
				}

				// Found viable row and column. See if node has already been traversed
				Node nextNode = new Node(row, col);

				if (!rollGraph.nodes.contains(nextNode)) {
					// Node not found, see if it passes the membership test
					String newWord = charsSoFar + roll.rollCharacters[row][col];

					// If Bloom is null (user set Bloom to not be used, just emit)
					// If Bloom is not null, do a membership test and emit
					proceed = bloomFilter == null ? true : bloomFilter.membershipTest(new Key(newWord.getBytes()));
					
					if (proceed) {
						// It might exist, create new object, add new node, and emit
						@SuppressWarnings("unchecked")
						ArrayList<Node> nextNodeList = (ArrayList<Node>) rollGraph.nodes.clone();
						nextNodeList.add(nextNode);

						RollGraphWritable nextGraphWritable = new RollGraphWritable(nextNodeList, false);

						context.write(new Text(newWord), nextGraphWritable);

						// Use counters to keep track of how many words were found so far
						context.getCounter("boggle", "words").increment(1);
					} else {
						// Use counters to keep track of how many words were thrown out by the Bloom Filter
						context.getCounter("boggle", "bloom").increment(1);

						if (logger.isDebugEnabled()) {
							logger.debug("Throwing out " + newWord + " because it didn't pass membership test");
						}
					}
				}
			}
		}
	}
}
