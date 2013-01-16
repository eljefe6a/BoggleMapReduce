import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class BoggleDriver extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger("Boggle");

	/** The parameter name for the minimum word size to output */
	public static final String MINIMUM_WORD_SIZE_PARAM = "minimumwordsize";

	/** The default value for the minimum word size to output */
	public static final int MINIMUM_WORD_SIZE_DEFAULT = 3;

	/** The parameter name for the bloom filter location */
	public static final String BLOOM_PARAM = "bloompath";

	/** The parameter name for the dictionary location */
	public static final String DICTIONARY_PARAM = "dictionarypath";

	/** The parameter name for the roll to be serialized */
	public static final String ROLL_PARAM = "roll";

	/** The parameter name for the roll version */
	public static final String ROLL_VERSION_PARAM = "rollversion";
	
	/** The parameter name for the roll version */
	public static final int ROLL_VERSION_DEFAULT = BoggleRoll.NEW_VERSION;

	/** The parameter name for the minimum word size to output */
	public static final String MAX_ITERATIONS_PARAM = "maxiterations";

	/** The default value for the minimum word size to output */
	public static final int MAX_ITERATIONS_DEFAULT = 15;

	/** The parameter name to use a BloomFilter */
	public static final String ENABLE_BLOOM_PARAM = "bloom";

	/** The default value for using the BloomFilter */
	public static final boolean ENABLE_BLOOM_DEFAULT = true;
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.out.println("Usage: BoggleDriver <bloomfile> <dictionary> <input dir> <output dir>");
			return -1;
		}

		String bloomPath = args[0];
		String dictionary = args[1];
		String input = args[2];
		String output = args[3];

		// Change Log4J to only output the map and reducer data
		// This helps view what's happening with the data easier
		// Comment this out to get all log data
		Logger.getLogger("Boggle").setLevel(Level.INFO);
		Logger.getRootLogger().setLevel(Level.ERROR);

		Configuration configuration = getConf();

		FileSystem fileSystem = FileSystem.get(configuration);

		if (!fileSystem.exists(new Path(bloomPath))) {
			// Verify that Bloom file exists
			System.out.println("Could not find bloom file");
			return -1;
		}

		if (fileSystem.exists(new Path(output))) {
			// Verify that output does not exist
			System.out.println("Output file already exists");
			return -1;
		}

		configuration.set(BLOOM_PARAM, bloomPath);
		configuration.set(DICTIONARY_PARAM, dictionary);
		configuration.setBooleanIfUnset(ENABLE_BLOOM_PARAM, ENABLE_BLOOM_DEFAULT);

		BoggleRoll roll = BoggleRoll.createRoll(configuration.getInt(ROLL_VERSION_PARAM, ROLL_VERSION_DEFAULT));
		configuration.set(ROLL_PARAM, roll.serialize());

		int iteration = traverseGraph(input, configuration, fileSystem, roll);

		boolean success = findWords(input, output, configuration, iteration, roll);

		return success ? 0 : 1;
	}

	/**
	 * Traverses the graph until all possible words are found
	 * 
	 * @param input
	 *            The input directory
	 * @param configuration
	 *            The configuration object
	 * @param fileSystem
	 *            The filesystem object
	 * @param roll
	 *            The Boggle roll to process
	 * @return The number of iterations it took to traverse the graph
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private int traverseGraph(String input, Configuration configuration, FileSystem fileSystem, BoggleRoll roll)
			throws IOException, InterruptedException, ClassNotFoundException {
		int iteration = 0;

		int maxiterations = configuration.getInt(MAX_ITERATIONS_PARAM, MAX_ITERATIONS_DEFAULT);

		writeRollFile(input, fileSystem, configuration, roll, iteration);

		long previousWordCount = 0;
		long bloomSavings = 0;
		
		logger.info("Starting graph traversal.");

		// Traverse the graph until it is completely traversed
		do {
			Job job = new Job(configuration);
			job.setJarByClass(BoggleDriver.class);
			job.setJobName("Boggle Graph Iteration " + iteration);

			FileInputFormat.setInputPaths(job, getPath(input, iteration));
			FileOutputFormat.setOutputPath(job, getPath(input, iteration + 1));

			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

			job.setNumReduceTasks(0);

			job.setMapperClass(BoggleMapper.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(RollGraphWritable.class);

			boolean success = job.waitForCompletion(true);

			if (!success) {
				throw new RuntimeException("Job did not return sucessfully.  Check the logs for info.");
			}

			long currentWordCount = job.getCounters().findCounter("boggle", "words").getValue();
			bloomSavings += job.getCounters().findCounter("boggle", "bloom").getValue();

			logger.info("Traversed graph for " + iteration + " iterations.  Found " + currentWordCount
					+ " potential words.  Bloom prevented " + bloomSavings + " traversals so far.");

			// Check to see if the entire graph has been traversed, the entire roll has been iterated,
			// or the maximum number of iterations have happened.
			if (currentWordCount == previousWordCount
					|| iteration == (roll.rollSize * roll.rollSize) || iteration == maxiterations) {
				logger.info("Finished traversing graph after " + iteration + " iterations.  Found " + currentWordCount
						+ " potential words.  Bloom prevented " + bloomSavings + " traversals.");
				break;
			}

			previousWordCount = currentWordCount;

			iteration++;
		} while (true);

		return iteration;
	}

	/**
	 * Takes the traversed graph and finds the actual words in the Boggle Roll
	 * 
	 * @param input
	 *            The input directory
	 * @param output
	 *            The output directory
	 * @param configuration
	 *            The configuration object
	 * @param iteration
	 *            The number of iterations it took to traverse the graph
	 * @param roll
	 *            The Boggle roll
	 * @return If the job was successful
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private boolean findWords(String input, String output, Configuration configuration, int iteration, BoggleRoll roll)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(configuration);
		job.setJarByClass(BoggleDriver.class);
		job.setJobName("Boggle Graph Final");

		FileInputFormat.setInputPaths(job, getPath(input, iteration));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(BoggleWordMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(RollGraphWritable.class);

		boolean success = job.waitForCompletion(true);

		long finalWordCount = job.getCounters().findCounter("boggle", "finalwords").getValue();

		logger.info("Finished traversing graph after " + iteration + " iterations.  Found " + finalWordCount
				+ " total words.  With roll:\n" + roll.toString());

		return success;
	}

	/**
	 * Writes out the Boggle roll to a file as an adjacency matrix
	 * 
	 * @param input
	 *            The place to write the roll to
	 * @param fileSystem
	 *            The filesystem object
	 * @param roll
	 *            The Boggle roll to write out
	 * @param iteration
	 *            The iteration for the input
	 * @throws IOException
	 */
	private void writeRollFile(String input, FileSystem fileSystem, Configuration configuration, BoggleRoll roll,
			int iteration) throws IOException {
		Path parent = getPath(input, iteration);
		fileSystem.mkdirs(parent);

		SequenceFile.Writer writer = null;
		
		for (int i = 0; i < roll.rollCharacters.length; i++) {
			writer = SequenceFile.createWriter(fileSystem, configuration, new Path(parent, i + ".txt"),
					Text.class, RollGraphWritable.class);
			
			for (int j = 0; j < roll.rollCharacters[i].length; j++) {
				ArrayList<Node> nodes = new ArrayList<Node>();
				nodes.add(new Node(i, j));

				RollGraphWritable graphWritable = new RollGraphWritable(nodes, false);

				Text text = new Text(roll.rollCharacters[i][j]);

				// Mimic the adjacency matrix written by the mapper to start things off
				writer.append(text, graphWritable);
			}
			
			IOUtils.closeStream(writer);
		}
		
		IOUtils.closeStream(writer);
	}

	/**
	 * Gets the path based on the iteration
	 * 
	 * @param input
	 *            The base input directory
	 * @param iteration
	 *            The iteration number
	 * @return The path for the iteration
	 */
	private Path getPath(String input, int iteration) {
		return new Path(input + "-" + iteration);
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new BoggleDriver(), args);
		System.exit(exitCode);
	}
}
