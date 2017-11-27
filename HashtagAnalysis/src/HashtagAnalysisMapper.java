

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class HashtagAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private Text data = new Text();
    private final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
         String[] fields = value.toString().split(";");

         if(fields.length == 4)
         {
               int tweetLength = fields[2].length();

               if(tweetLength<= 140)
               {
                   if(fields[0].length() == 13)
                   {
                	   try {
                		   LocalDateTime tweetDate = LocalDateTime.ofEpochSecond(Long.parseLong(fields[0])/1000,0,ZoneOffset.UTC);
                           data.set(String.valueOf(tweetDate.getHour()));
                           //context.write(data, one);

                           if(tweetDate.getHour() == 1){
                             String str = fields[2];
                             Pattern pat= Pattern.compile("#(\\w+)\\b");
                              Matcher mat = pat.matcher(str);

                              while (mat.find()) {

                                data.set(mat.group());

                                context.write(data, one);
                              }
                           }

                	   }catch(Exception e) {}

                   }
               }
         }

    }
}
