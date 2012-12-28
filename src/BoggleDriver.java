import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class BoggleDriver extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(BoggleDriver.class);

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

		Configuration configuration = getConf();
		// To change how the mappers are created to process the roll,
		// pass in -D mapreduce.input.lineinputformat.linespermap=0
		// or in code uncomment:
		// configuration.set("mapreduce.input.lineinputformat.linespermap", "8");

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

		BoggleRoll roll = BoggleRoll.createRoll();
		configuration.set(ROLL_PARAM, roll.serialize());

		int iteration = traverseGraph(input, configuration, fileSystem, roll);

		boolean success = findWords(input, output, configuration, iteration);

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

		writeRollFile(input, fileSystem, roll, iteration);

		long previousWordCount = 0;

		// Traverse the graph until it is completely traversed
		do {
			Job job = new Job(configuration);
			job.setJarByClass(BoggleDriver.class);
			job.setJobName("Boggle Graph Iteration " + iteration);

			FileInputFormat.setInputPaths(job, getPath(input, iteration));
			FileOutputFormat.setOutputPath(job, getPath(input, iteration + 1));

			// Roll is broken in to x mappers per node
			job.setInputFormatClass(NLineInputFormat.class);

			job.setNumReduceTasks(1);

			job.setMapperClass(BoggleMapper.class);
			job.setReducerClass(BoggleReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(RollGraphWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(RollGraphWritable.class);

			boolean success = job.waitForCompletion(true);

			if (!success) {
				throw new RuntimeException("Job did not return sucessfully.  Check the logs for info.");
			}

			// Check to see if the entire graph has been traversed
			long currentWordCount = job.getCounters().findCounter("boggle", "words").getValue();

			if (currentWordCount == previousWordCount) {
				logger.info("Finished traversing graph after " + iteration + " iterations.  Found " + currentWordCount
						+ " potential words.");
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
	 * @return If the job was successful
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private boolean findWords(String input, String output, Configuration configuration, int iteration)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(configuration);
		job.setJarByClass(BoggleDriver.class);
		job.setJobName("Boggle Graph Final");

		FileInputFormat.setInputPaths(job, getPath(input, iteration));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setNumReduceTasks(1);

		job.setMapperClass(BoggleWordMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(RollGraphWritable.class);

		boolean success = job.waitForCompletion(true);
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
	private void writeRollFile(String input, FileSystem fileSystem, BoggleRoll roll, int iteration) throws IOException {
		FSDataOutputStream outputStream = fileSystem.create(getPath(input, 0));

		for (int i = 0; i < roll.rollCharacters.length; i++) {
			for (int j = 0; j < roll.rollCharacters[i].length; j++) {
				ArrayList<Node> nodes = new ArrayList<Node>();
				nodes.add(new Node(i, j));
				
				RollGraphWritable graphWritable = new RollGraphWritable(nodes, false);
				
				// Mimic the adjacency matrix written by the mapper to start things off
				String output = roll.rollCharacters[i][j] + " " + graphWritable.serialize() + "\n";
				outputStream.writeBytes(output);
			}
		}

		outputStream.close();
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
