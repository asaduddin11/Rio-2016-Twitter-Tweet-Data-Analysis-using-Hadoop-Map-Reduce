import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TweetLengthMapper extends Mapper<Object, Text, IntIntPair, IntWritable> {

private IntIntPair data = new IntIntPair();
private IntWritable numInstances = new IntWritable(1);
private int tweetLng;


    public void map(Object key, Text value, Context context) throws IOException, 		InterruptedException {

	String[] inputLines = value.toString().split(";");
	try{
	if (inputLines.length == 4) {
		tweetLng = inputLines[2].length(); //count length
		if(tweetLng<=140 && tweetLng>=1)
		{
			for(int i=5; i<=140; i+=5)
			{
				if(tweetLng<=i && tweetLng>i-5)
				{
				int min = i-4;
				int max = i;


		data.set(new IntWritable(min), new IntWritable(max));
		context.write(data,numInstances);
				}
			}
		}
	}



	}
	catch(Exception e){
		System.out.println("Error: " + e);
	}

        }
    }
