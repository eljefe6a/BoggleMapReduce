import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BoggleReducer extends Reducer<Text, RollGraphWritable, Text, RollGraphWritable> {

	@Override
	public void reduce(Text key, Iterable<RollGraphWritable> values, Context context) throws IOException,
			InterruptedException {
		for (RollGraphWritable value : values) {
			context.write(key, value);
			
			context.getCounter("boggle", "words").increment(1);
		}
	}
}