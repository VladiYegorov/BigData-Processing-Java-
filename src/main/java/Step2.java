
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//finding all dependency paths  
//input: step1 - w1 w2 boolean deplist
//output example: prep (dep vector)
public class Step2 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            while (it.hasMoreTokens()) {
                String[] line = it.nextToken().split("\t");
                for(int i=2;i<line.length;i+=2) {
                	context.write(new Text(line[i].trim()),new Text(line[1])); //adding "partmod made prep in pobj	True"
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {  
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
        	int totalCount = 0;
        	int NanCount = 0;
        	for(Text it : values) {
        		if(it.toString().equals("Nan")) 
        			NanCount++;
        		totalCount++;
        	}
        	if(NanCount != totalCount && totalCount >= Integer.parseInt(context.getConfiguration().get("DPmin","10"))) {
        		context.write(key,new Text()); //adding "partmod made prep in pobj	" if the path count > DPmin and the path appear in at least one True/False test set. 
        	}
        }
    }

    
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	conf.set("DPmin", args[2]);
    	
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapperClass(MapperClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);	
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);        
    }
}

