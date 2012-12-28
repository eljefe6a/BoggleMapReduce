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
	/** Each dice in Boggle only certain characters. Each character represents a side of the dice */
	public static final String[] letters = { "aaeegn", "elrtty", "aoottw", "abbjoo", "ehrtvw", "cimotv", "distty",
			"eiosst", "delrvy", "achops", "humnqu", "eeinsu", "eeghnw", "affkps", "hlnnrz", "deilrx" };
	
	/** The size of the square for the Boggle roll */
	private static final int rollSize = 4;

	/** The characters that were chosen in the Boggle roll */
	public String[][] rollCharacters;

	/**
	 * Private constructor. Use static initializers.
	 */
	private BoggleRoll() {
		rollCharacters = new String[letters.length / rollSize][letters.length / rollSize];
	}

	/**
	 * Creates a random roll of Boggle based on the possible letters
	 * 
	 * @return A Boggle roll
	 */
	public static BoggleRoll createRoll() {
		// Shuffle the dice around
		ArrayList<Integer> diceList = new ArrayList<Integer>();

		for (int i = 0; i < letters.length; i++) {
			diceList.add(i);
		}

		Collections.shuffle(diceList);

		Random random = new Random();

		int numSides = letters[0].length();

		BoggleRoll roll = new BoggleRoll();

		// Choose a side of the dice
		for (int i = 0; i < diceList.size(); i++) {
			int index = random.nextInt(numSides);

			String letter = letters[diceList.get(i)].substring(index, index + 1);

			// Set the chosen letter in the characters array
			roll.rollCharacters[i / rollSize][i % rollSize] = letter;
		}

		return roll;
	}

	/**
	 * Serializes the Boggle Roll to a string
	 * @return A string representation of the roll
	 */
	public String serialize() {
		return toString();
	}

	/**
	 * Deserializes the Boggle Roll from a string
	 * @param rollString The string representation of the roll
	 * @return The roll object based on the string
	 */
	public static BoggleRoll deserialize(String rollString) {
		// Split the roll in to lines for the rows
		String[] lines = rollString.split("\n");

		BoggleRoll roll = new BoggleRoll();

		for (int i = 0; i < lines.length; i++) {
			// Split the row in to columns
			String[] letters = lines[i].split(",");

			for (int j = 0; j < letters.length; j++) {
				// Set the character for the roll
				roll.rollCharacters[i][j] = letters[j];
			}
		}

		return roll;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();

		for (int i = 0; i < rollCharacters.length; i++) {
			for (int j = 0; j < rollCharacters[i].length; j++) {
				buffer.append(rollCharacters[i][j]).append(",");
			}

			buffer.append("\n");
		}

		return buffer.toString();
	}
}
