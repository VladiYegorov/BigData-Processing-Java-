
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//making dependency vector for each NN to NN. 
//input: step2 - dep (dep vector)
//input: step1 - w1 w2	Boolean	depList
//output example: a b	True	0	0	15	0	10
public class Step3 {
    
	public static class DepMapper extends Mapper<LongWritable, Text, PrioDep, Text> {	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            while (it.hasMoreTokens()) {
                String line = it.nextToken().split("\t",2)[0].trim();
                for(int i=0;i<context.getNumReduceTasks();i++)
                	context.write(new PrioDep(1,i,line),new Text());
            }
        }
    }
	
    public static class WordsMapper extends Mapper<LongWritable, Text, PrioDep, Text> {	
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            while (it.hasMoreTokens()) {
                String[] line = it.nextToken().split("\t",3);
                if(!line[1].equals("Nan"))
                	context.write(new PrioDep(2,0,line[0]),new Text(line[1] + "\t" + line[2]));
            }
        }
    }

    public static class JoinReducerClass extends Reducer<PrioDep,Text,Text,Text> {
    	
    	HashMap<Integer,String> depVec = new HashMap<Integer,String>();
    	
        @Override
		public void reduce(PrioDep key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	if(key.getPrio().get() == 1) {
        		depVec.put(depVec.size(),key.getKey().toString().trim());	
        		return;
        	}
			String newValue = "";
			HashMap<String, String> wordsDep = new HashMap<String, String>();		
			
			for (Text it : values) {
				String[] value = it.toString().split("\t");
				newValue = value[0];
				for (int i = 1; i < value.length; i += 2) 
					wordsDep.put(value[i], value[i + 1]);
			}	
			for (int i = 0; i < depVec.size(); i++) {
				if (wordsDep.containsKey(depVec.get(i)))
					newValue += "\t" + wordsDep.get(depVec.get(i));
				else
					newValue += "\t" + "0";
			}
			context.write(key.getKey(), new Text(newValue));
		}
	}
    
    public static class PartitionerClass extends Partitioner<PrioDep, Text> {
        @Override
        public int getPartition(PrioDep key, Text value, int numPartitions) { 
        	if(key.getPrio().get()==1)
        		return key.getPartition().get() % numPartitions;
            return (key.getKey().toString().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);
        job.setReducerClass(JoinReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);  
        job.setMapOutputKeyClass(PrioDep.class);
        job.setMapOutputValueClass(Text.class);
        
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WordsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DepMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class PrioDep implements WritableComparable<PrioDep> {
	IntWritable prio;
	IntWritable partition;
	Text key;
    
	PrioDep(){
		this.prio = new IntWritable();
		this.partition = new IntWritable();
		this.key = new Text();
	}
	
	PrioDep(int prio,int partition, String key) {
		this.prio = new IntWritable(prio);
		this.partition = new IntWritable(partition);
        this.key = new Text(key);   
    }     
	
	public IntWritable getPrio() {
    	return prio;
    }
	
	public IntWritable getPartition() {
    	return partition;
    }
    
    public Text getKey() {
    	return key;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	prio.readFields(in); 
    	partition.readFields(in); 
        key.readFields(in);  
    }
    @Override
    public void write(DataOutput out) throws IOException {
    	prio.write(out);
    	partition.write(out);
    	key.write(out);
    }
    
    @Override
    public int compareTo(PrioDep other) {
    	int result = prio.get() - other.prio.get();
    	if(result == 0) 
    		result = other.key.toString().compareTo(key.toString()); 	
        return result;       	
    } 
    
    public String toString() { 
  	  return key.toString();  
  } 
}
