import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

/**
 * Represents a roll in Boggle.
 * 
 * @author jesseanderson
 * 
 */
public class BoggleRoll {
	// Each dice in Boggle only certain characters. Each character represents a side of the dice
	public static final int newVersion = 0;

	public static final int oldVersion = 1;

	public static final int bigBoggleVersion = 2;

	/** Dice for the new version of Boggle */
	public static final String[] newVersionDice = { "aaeegn", "elrtty", "aoottw", "abbjoo", "ehrtvw", "cimotv",
			"distty", "eiosst", "delrvy", "achops", "humnqu", "eeinsu", "eeghnw", "affkps", "hlnnrz", "deilrx" };

	/** Dice for the old version of Boggle */
	public static final String[] oldVersionDice = { "aaciot", "ahmors", "egkluy", "abilty", "acdemp", "egintv",
			"gilruw", "elpstu", "denosw", "acelrs", "abjmoq", "eefhiy", "ehinps", "dknotu", "adenvz", "biforx" };

	/** Dice for the Big Boggle version */
	public static final String[] bigBoggleVersionDice = { "aaafrs", "aaeeee", "aafirs", "adennn", "aeeeem", "aeegmu",
			"aegmnn", "afirsy", "bjkqxz", "ccenst", "ceiilt", "ceilpt", "ceipst", "ddhnot", "dhhlor", "dhlnor",
			"dhlnor", "eiiitt", "emottt", "ensssu", "fiprsy", "gorrvw", "iprrry", "nootuw", "ooottu" };

	/** The number of sides on a dice */
	private static final int diceSides = 6;

	/** The version of the dice to use */
	public String[] versionDice;

	/** The size of the square for the Boggle roll */
	public int rollSize;

	/** The characters that were chosen in the Boggle roll */
	public String[][] rollCharacters;
	
	/** The version used for this Boggle Roll */
	private int version;

	/**
	 * Private constructor. Use static initializers.
	 * 
	 * @param version
	 *            The version or size of the dice to use. A version >5 will create a random version with a dimension of
	 *            version x version.
	 */
	private BoggleRoll(int version) {
		this.version = version;
		
		if (version == newVersion) {
			versionDice = newVersionDice;
		} else if (version == oldVersion) {
			versionDice = oldVersionDice;
		} else if (version == bigBoggleVersion) {
			versionDice = bigBoggleVersionDice;
		} else if (version > 5) {
			createRandomVersion(version);
		} else {
			// Versions 3-5 are not valid sizes because they aren't big enough
			throw new RuntimeException("The version for the Boggle Roll is not valid.  The version was " + version);
		}

		rollSize = (int) Math.sqrt(versionDice.length);
		rollCharacters = new String[rollSize][rollSize];
	}

	private void createRandomVersion(int size) {

	}

	/**
	 * Creates a random roll of Boggle based on the possible version dice
	 * 
	 * @param version
	 *            The version or size of the dice to use. A version >5 will create a random version with a dimension of
	 *            version x version.
	 * @return A Boggle roll
	 */
	public static BoggleRoll createRoll(int version) {
		// Shuffle the dice around
		ArrayList<Integer> diceList = new ArrayList<Integer>();

		BoggleRoll roll = new BoggleRoll(version);
		
		for (int i = 0; i < roll.versionDice.length; i++) {
			diceList.add(i);
		}

		Collections.shuffle(diceList);

		Random random = new Random();

		// Choose a side of the dice
		for (int i = 0; i < diceList.size(); i++) {
			int index = random.nextInt(diceSides);

			String letter = roll.versionDice[diceList.get(i)].substring(index, index + 1);

			if (letter.equals("q")) {
				// Boggle had special dice sides where a "q" had a "u"
				// So the dice would read "qu"
				letter = "qu";
			}

			// Set the chosen letter in the characters array
			roll.rollCharacters[i / roll.rollSize][i % roll.rollSize] = letter;
		}

		return roll;
	}

	/**
	 * Serializes the Boggle Roll to a string
	 * 
	 * @return A string representation of the roll
	 */
	public String serialize() {
		return toString();
	}

	/**
	 * Deserializes the Boggle Roll from a string
	 * 
	 * @param rollString
	 *            The string representation of the roll
	 * @return The roll object based on the string
	 */
	public static BoggleRoll deserialize(String rollString) {
		// Split the roll in to lines for the rows
		String[] lines = rollString.split("\n");

		BoggleRoll roll = new BoggleRoll(Integer.parseInt(lines[0]));

		for (int i = 1; i < lines.length; i++) {
			// Split the row in to columns
			String[] letters = lines[i].split(",");

			for (int j = 0; j < letters.length; j++) {
				// Set the character for the roll
				roll.rollCharacters[i - 1][j] = letters[j];
			}
		}

		return roll;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();

		// Add matrix size
		buffer.append(version).append("\n");
		
		for (int i = 0; i < rollCharacters.length; i++) {
			for (int j = 0; j < rollCharacters[i].length; j++) {
				buffer.append(rollCharacters[i][j]).append(",");
			}

			buffer.append("\n");
		}

		return buffer.toString();
	}
}
