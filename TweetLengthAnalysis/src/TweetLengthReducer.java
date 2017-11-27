import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Reducer;


public class TweetLengthReducer extends Reducer<IntIntPair, IntWritable, IntIntPair, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(IntIntPair key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {

		sum+=value.get();

        }

        result.set(sum);

	context.write(key, result);

    }


}
