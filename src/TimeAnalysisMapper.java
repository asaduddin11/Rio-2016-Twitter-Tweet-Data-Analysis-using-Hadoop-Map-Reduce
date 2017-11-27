import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.SimpleDateFormat;

public class TimeAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {
private IntWritable numInstances = new IntWritable(1);
String tweetDate;

    public void map(Object key, Text value, Context context) throws IOException, 		InterruptedException {

	String[] inputLines = value.toString().split(";");

		try{
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH");
	    Date tweetsdate1 = new Date(Long.parseLong(inputLines[0]));
		tweetDate = dateFormat.format(tweetsdate1);
		for(int i=0;i<=23;i++)
		{
      if(Integer.parseInt(tweetDate) == i)
		    {
      context.write(new Text(tweetDate),numInstances);

		    }
    }
  }
		catch (Exception e){}

   }

}
