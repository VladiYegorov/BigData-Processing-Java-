import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//finding all NN to NN combination.
//input: BoolMapper - w1 w2 boolean
//input: SentanceMapper - syntactic NGRAM
//output example: a b	True	prep	15
public class Step1 {
	public static class BoolMapper extends Mapper<LongWritable, Text, Text, Text> {
            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
                while (itr.hasMoreTokens()) {
                	String[] line = itr.nextToken().split("\t"); 
                	Stemmer s = new Stemmer();
                	String newKey = "";
                	s.add(line[0].toCharArray(), line[0].length()); s.stem();
                	newKey = s.toString();
                	s.add(line[1].toCharArray(), line[1].length()); s.stem();
                	newKey +=" " +s.toString();
                    context.write(new Text(newKey), new Text(line[2])); // adding "w1 w2	True"
                }
            }
        }

	public static class SentanceMapper extends Mapper<LongWritable, Text, Text, Text> {
          
        static final String[] nouns = {"NN","NNS","NNP","NNPS"};
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) { 
                String[] line = itr.nextToken().split("\t");
                String[] words = line[1].split("\\s+");          
                for(int i=0; i<words.length; i++)
                	if(Arrays.stream(nouns).anyMatch(words[i].split("/")[1]::equals))  {
                		List<String> depList = makeDependencyPath(words,i);	// find all dependency paths from specific noun to all other nouns.
                		for(String it : depList) {
                			String newkey = it.split("\t")[0];
                			String newvalue = it.split("\t")[1] + "\t" + line[2];
                			context.write(new Text(newkey),new Text(newvalue)); // adding "w1 w2	several/JJ/amod/3 other/JJ/amod/3 actions/NNS/nsubj/0"	
                		}   		           
                }
            }
        }
        
        private List<String> makeDependencyPath(String[] words,int secondWordIndex) {
        	List<String> result = new ArrayList<String>(); 
        	String currPath = null;	
        	String secondWord = words[secondWordIndex].split("/")[0];
        	int prev = secondWordIndex;
        	int curr = Integer.parseInt(words[secondWordIndex].split("/")[3])-1;
        	int count = 0;
        	while(curr >= 0 && count < 5){  
        		String[] currData = words[curr].split("/");
        		if(currData.length != 4 && currData.length != 5) {
        			break;
        		}
        		if(currData.length == 5) {
        			currData[0] = "/";
        			currData[1] = currData[2];
        			currData[2] = currData[3];
        			currData[3] = currData[4];
        		}
        		Stemmer s = new Stemmer();
            	s.add(currData[0].toCharArray(), currData[0].length()); s.stem();
            	currData[0] = s.toString();    
            	
				if (Arrays.stream(nouns).anyMatch(currData[1]::equals)) {
					s.add(secondWord.toCharArray(), secondWord.length()); s.stem();
					result.add(currData[0] + " " + s.toString() + "\t" + words[prev].split("/")[2].trim() + (currPath != null ? " " + currPath : ""));
				}
        		currPath = currData[0] + " " + words[prev].split("/")[2].trim() + (currPath!=null ? " "+ currPath : "");
        		prev = curr;
        		curr = Integer.parseInt(currData[3])-1;
        		count++;
        	}
        	return result;
        }
    }
    	
    	public static class ReduceJoinReducer extends Reducer<Text,Text,Text,Text> {

    	
            @Override
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            	HashMap<String,Integer> dep = new HashMap<String,Integer>();
            	String isHyp = "Nan";
            	String result = null;
            	for (Text value : values) {
            		if(value.toString().equals("True") || value.toString().equals("False")) {
            			isHyp = value.toString();
            		} else {
            			//calculating the total amount of a specific path. 
            			String[] valueSplit = value.toString().split("\t");
            			if(dep.containsKey(valueSplit[0])) {
            				dep.compute(valueSplit[0], (k, v) -> v+=Integer.parseInt(valueSplit[1]));
            			} else {
            				dep.put(valueSplit[0], Integer.parseInt(valueSplit[1]));
            			}
            		}		
            	}    	          
            	if(!dep.isEmpty()) {
            		for (Map.Entry<String, Integer> entry : dep.entrySet()) {
            		    if(result == null)
            		    	result = isHyp + "\t" + entry.getKey() + "\t" + entry.getValue().toString();
            		    else
            		    	result = result + "\t" + entry.getKey() + "\t" + entry.getValue().toString();
            		}
            		context.write(key, new Text(result)); //adding "adjust respons	False	partmod made prep in pobj	30	prep in pobj	14"		
            	}	         
            }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);
        job.setReducerClass(ReduceJoinReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);  
        
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, BoolMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SentanceMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}