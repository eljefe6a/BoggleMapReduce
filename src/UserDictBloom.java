import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class UserDictBloom {
	/** The vector size for the Bloom Filter */
	public static final int VECTOR_SIZE = 10485760;
	/** The number of hashes for the Bloom Filter */
	public static final int NBHASH = 6;
	/** The type of hashing to use for the Bloom Filter */
	public static final int HASH_TYPE = Hash.MURMUR_HASH;

	public static void main(String args[]) {
		try {
			String wordFile = null;
			String bloomFile = null;

			if (args.length == 2) {
				wordFile = args[0];
				bloomFile = args[1];
			} else if (args.length == 0) {
				wordFile = "/usr/share/dict/words";
				bloomFile = "bloom.out";
			} else {
				System.out.println("Usage <pathtodictionary> <pathtobloomfile>");
			}

			System.out.println("Reading dictionary from " + wordFile + " outputting Bloom Filter to " + bloomFile);

			// Go through every word in the words file
			BufferedReader dict = new BufferedReader(new FileReader(wordFile));

			String line;

			Pattern words = Pattern.compile("[a-z]*");

			BloomFilter bloomFilter = new BloomFilter(VECTOR_SIZE, NBHASH, HASH_TYPE);

			while ((line = dict.readLine()) != null) {
				// Normalize all words to lower case and remove all dashes
				line = line.toLowerCase().replace("-", "");
				Matcher matcher = words.matcher(line);

				if (matcher.matches()) {
					// Add to Bloom Filter breaking up the word along the way
					for (int i = 0; i < line.length(); i++) {
						String wordPiece = line.substring(0, i + 1);
						bloomFilter.add(new Key(wordPiece.getBytes()));
					}
				} else {
					System.out.println("Skipping entry: \"" + line + "\"");
				}
			}

			dict.close();

			// Write out the Bloom Filter to a file
			Configuration configuration = new Configuration();
			FileSystem fs = FileSystem.get(configuration);

			DataOutputStream outputStream = fs.create(new Path(bloomFile));
			bloomFilter.write(outputStream);

			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
