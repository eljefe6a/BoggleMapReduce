import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class BoggleRoll {
	public static final String[] letters = { "aaeegn", "elrtty", "aoottw", "abbjoo", "ehrtvw", "cimotv", "distty",
			"eiosst", "delrvy", "achops", "humnqu", "eeinsu", "eeghnw", "affkps", "hlnnrz", "deilrx" };

	public String[][] rollCharacters;

	private BoggleRoll() {
		rollCharacters = new String[letters.length / 4][letters.length / 4];
	}

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

			roll.rollCharacters[i / 4][i % 4] = letter;
		}

		return roll;
	}

	public String serialize() {
		return toString();
	}

	public static BoggleRoll deserialize(String rollString) {
		String[] lines = rollString.split("\n");

		BoggleRoll roll = new BoggleRoll();

		for (int i = 0; i < lines.length; i++) {
			String[] letters = lines[i].split(",");

			for (int j = 0; j < letters.length; j++) {
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
