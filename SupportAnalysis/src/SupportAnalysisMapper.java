


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SupportAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {
        private HashSet<String> athletesInfo;
        private IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] name = value.toString().split(";");
                // here we match the two tables in the joins
        try
        {
        	if (name.length != 4)
				throw new Exception();
			for (String str : athletesInfo) {
				if (name[2].toLowerCase().contains(str))
					context.write(new Text(str), one);
			}
        }
        catch(Exception e)
        {
                System.out.println(value);
        }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
                athletesInfo = new HashSet<String>();
                // We know there is only one cache file, so we only retrieve that URI
                URI fileUri = context.getCacheFiles()[0];
                FileSystem fs = FileSystem.get(context.getConfiguration());
                FSDataInputStream in = fs.open(new Path(fileUri));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line = null;
                try {
                        // we discard the header row
                        br.readLine();
                        while ((line = br.readLine()) != null) {
                                context.getCounter(CustomCounters.NUM_ATHLETES).increment(1);
                                String[] fields = line.split(",");
                                //id(0),name(1),nationality(2),sex(3),dob(4),height(5),weight(6),sport(7),gold(8),silver(9),bronze(10)
                                if (fields.length == 11)
                                        athletesInfo.add(fields[1].toLowerCase());
                        }
                        br.close();
                } catch (IOException e1) {
                }

                super.setup(context);
        }

}
