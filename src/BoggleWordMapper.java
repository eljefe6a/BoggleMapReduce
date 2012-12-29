import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class BoggleWordMapper extends Mapper<Text, RollGraphWritable, Text, RollGraphWritable> {
	private static final Logger logger = Logger.getLogger("Boggle");

	/** All words from the dictionary */
	private HashSet<String> words = new HashSet<String>();

	/** The minimum size for a word to be output */
	private int minimumWordSize = 0;

	@Override
	public void setup(Context context) throws IOException {
		Configuration configuration = context.getConfiguration();

		// Open the dictionary file
		FileSystem fileSystem = FileSystem.get(configuration);
		FSDataInputStream dict = fileSystem.open(new Path(configuration.get(BoggleDriver.DICTIONARY_PARAM)));

		String line;

		Pattern wordsPattern = Pattern.compile("[a-z]*");

		while ((line = dict.readLine()) != null) {
			// Normalize all words to lower case and remove all dashes
			line = line.toLowerCase().replace("-", "");
			Matcher matcher = wordsPattern.matcher(line);

			if (matcher.matches()) {
				// Add the word to the HashSet for quick checks
				words.add(line);
			} else {
				if (logger.isDebugEnabled()) {
					logger.debug("Skipping entry: \"" + line + "\"");
				}
			}
		}

		dict.close();

		// Get the minimum word size from the configuration
		minimumWordSize = configuration.getInt(BoggleDriver.MINIMUM_WORD_SIZE_PARAM,
				BoggleDriver.MINIMUM_WORD_SIZE_DEFAULT);
	}

	@Override
	public void map(Text key, RollGraphWritable value, Context context) throws IOException, InterruptedException {
		String charsSoFar = key.toString();

		// See if the word is big enough to emit
		if (charsSoFar.length() >= minimumWordSize) {
			// See if the word actually appears in the dictionary
			if (words.contains(charsSoFar)) {
				// Word appears, emit
				context.write(new Text(charsSoFar), value);

				context.getCounter("boggle", "finalwords").increment(1);
			}
		}
	}
}
