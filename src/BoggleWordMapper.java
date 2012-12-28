import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class BoggleWordMapper extends Mapper<LongWritable, Text, Text, RollGraphWritable> {
	private static final Logger logger = Logger.getLogger(BoggleWordMapper.class);

	private BoggleRoll roll;

	private HashSet<String> words = new HashSet<String>();

	@Override
	public void setup(Context context) throws IOException {
		Configuration configuration = context.getConfiguration();

		roll = BoggleRoll.deserialize(configuration.get("roll"));

		FileSystem fileSystem = FileSystem.get(configuration);
		FSDataInputStream dict = fileSystem.open(new Path(configuration.get("dictionarypath")));

		String line;

		Pattern wordsPattern = Pattern.compile("[a-z]*");

		while ((line = dict.readLine()) != null) {
			// Normalize all words to lower case and remove all dashes
			line = line.toLowerCase().replace("-", "");
			Matcher matcher = wordsPattern.matcher(line);

			if (matcher.matches()) {
				words.add(line);
			} else {
				if (logger.isDebugEnabled()) {
					logger.debug("Skipping entry: \"" + line + "\"");
				}
			}
		}

		dict.close();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Expected input:
		// aaaaa [[0,0][1,1][2,2]] false
		String line = value.toString();

		String values[] = line.split("\\s");

		if (values.length == 3) {
			String charsSoFar = values[0];

			if (charsSoFar.length() >= BoggleDriver.MINIMUM_WORD_SIZE) {
				// See if the word actually appears in the dictionary
				if (words.contains(charsSoFar)) {
					RollGraphWritable rollGraph = RollGraphWritable.deserialize(values[1] + " " + values[2]);
	
					context.write(new Text(charsSoFar), rollGraph);
				}
			}
		} else {
			logger.warn("The input line had more spaces than were expected.  Had " + values.length
					+ " expected 3.  The line was \"" + line + "\"");
		}
	}
}
