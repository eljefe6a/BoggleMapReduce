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
	/** Constant for new version of Boggle */
	public static final int NEW_VERSION = 0;

	/** Constant for old version of Boggle */
	public static final int OLD_VERSION = 1;

	/** Constant for Big Boggle version */
	public static final int BIG_BOGGLE_VERSION = 2;

	// Each dice in Boggle only certain characters. Each character represents a side of the dice
	/** Dice for the new version of Boggle */
	public static final String[] NEW_VERSION_DICE = { "aaeegn", "elrtty", "aoottw", "abbjoo", "ehrtvw", "cimotv",
			"distty", "eiosst", "delrvy", "achops", "humnqu", "eeinsu", "eeghnw", "affkps", "hlnnrz", "deilrx" };

	/** Dice for the old version of Boggle */
	public static final String[] OLD_VERSION_DICE = { "aaciot", "ahmors", "egkluy", "abilty", "acdemp", "egintv",
			"gilruw", "elpstu", "denosw", "acelrs", "abjmoq", "eefhiy", "ehinps", "dknotu", "adenvz", "biforx" };

	/** Dice for the Big Boggle version */
	public static final String[] BIG_BOGGLE_VERSION_DICE = { "aaafrs", "aaeeee", "aafirs", "adennn", "aeeeem", "aeegmu",
			"aegmnn", "afirsy", "bjkqxz", "ccenst", "ceiilt", "ceilpt", "ceipst", "ddhnot", "dhhlor", "dhlnor",
			"dhlnor", "eiiitt", "emottt", "ensssu", "fiprsy", "gorrvw", "iprrry", "nootuw", "ooottu" };

	/** The number of sides on a dice */
	private static final int DICE_SIDES = 6;

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
		
		if (version == NEW_VERSION) {
			versionDice = NEW_VERSION_DICE;
		} else if (version == OLD_VERSION) {
			versionDice = OLD_VERSION_DICE;
		} else if (version == BIG_BOGGLE_VERSION) {
			versionDice = BIG_BOGGLE_VERSION_DICE;
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
		versionDice = new String[size*size];
		
		Random random = new Random();
		
		// Big Note!
		// Only the BoggleRoll created by the driver will have the correct version
		// of the dice.  You could serialize the object out and read it in, but
		// isn't necessary for this program.  We only need to make sure that
		// the dice sides are serialized correctly (they are).
		
		for (int i = 0; i < versionDice.length; i++) {
			versionDice[i] = BIG_BOGGLE_VERSION_DICE[random.nextInt(BIG_BOGGLE_VERSION_DICE.length)];
		}
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
			int index = random.nextInt(DICE_SIDES);

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
