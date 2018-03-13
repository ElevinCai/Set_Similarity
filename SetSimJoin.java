package comp9313.ass4;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SetSimJoin extends Configured implements Tool{


    public static String OUT = "output";
    public static String IN = "input";
   // public static double SIMILARITY_THRESHOLD = 0.0;
    public static  int REDUCER_NUM  = 0;


    public static class Pair implements WritableComparable<Pair>{
        private String key;
        private String vals;

        public Pair(){}

        public String getId(){
            return this.key;
        }

        public String getVals(){
            return this.vals;
        }

        public void setPair(String left ,String right){
            this.key = left;
            this.vals = right;
        }

        public void readFields(DataInput in) throws IOException{
            this.key = in.readUTF();
            this.vals = in.readUTF();
        }

        public void write(DataOutput out) throws IOException{
            out.writeUTF(key);
            out.writeUTF(vals);
        }

	

		@Override
		public int compareTo(Pair p) {
			int a = Integer.parseInt(this.getId());
			int c = Integer.parseInt(this.getVals());
			int b = Integer.parseInt(p.getId());
			int d = Integer.parseInt(p.getVals());
			if (a == b){
				if (c == d){
					return 0;
				}else{
					return Integer.compare(c, d);
				}
			}else{
				return Integer.compare(a, b);
			}
		}

	

        
    }



    // the FSMapper handles stage 2 of the Similarity Join,which compute the similarity using
    // Jaccard Similarity formula
    public static class FSMapper extends Mapper<Object, Text, Text, Text> {
    	Text valpairs  = new Text();
    	Text keys = new Text();
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       		String[] record = value.toString().split("\t| ");
       		String rid = record[0];
       		String val="";
       		
       		Configuration conf = context.getConfiguration();
       		Double SIMILARITY_THRESHOLD = Double.parseDouble(conf.get("threshold"));
       		//System.out.println(SIMILARITY_THRESHOLD);
            //get the record without record id

			for(int i = 1;i < record.length; i++ ){
                if(i == 1){
                    val +=record[i];
                }else{
                    val +="\t" + record[i];
                }
       			
       		}
            // compute prefix_length for each record
       		int prefix_length = (int)((record.length - 1) *(1 - SIMILARITY_THRESHOLD) + 1);
       		
            // emit key value pair,for example(A,(1,ABC))
       		
       		
       		//System.out.println(prefix_length);
       			for(int i= 1;i < prefix_length + 1;i++){
       				valpairs.set(rid.toString()+","+val.toString());
       			//System.out.println("i =  "+ i+" record[i] = " +record[i]);
       				keys.set(record[i]);
 	
       				
       				context.write(keys,valpairs);
       			}
       		

        }

    }
    public static class S2Partitioner extends
    Partitioner < Text, Text >
    {
       @Override
       public int getPartition(Text key, Text value, int numReduceTasks)
       {
    	  
         return key.toString().hashCode()% numReduceTasks;
       }
    }

    public static class FSReducer extends Reducer<Object, Text, Text, Text> {
    	Text result = new Text();
        Text k = new Text();
        public void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double r = 0.0;
            Configuration conf = context.getConfiguration();
       		Double SIMILARITY_THRESHOLD = Double.parseDouble(conf.get("threshold"));
        	ArrayList<String> buffer = new ArrayList<String>();
                for (Text v : values){

                	if (buffer.isEmpty()){
                		buffer.add(v.toString());
                	}else{         
                		ListIterator<String> iter = buffer.listIterator();
                		while(iter.hasNext()){
                			iter.add(v.toString());
                			String p = iter.next();
                			r =simcalculator(p.split(",")[1],v.toString().split(",")[1]);
                			if(r >= SIMILARITY_THRESHOLD){
                				int a = Integer.parseInt(p.split(",")[0]);
                				int b = Integer.parseInt(v.toString().split(",")[0]);
                				if (a<b){
                					k.set(p.split(",")[0]+"\t"+ v.toString().split(",")[0]);
                				}else{
                					k.set(v.toString().split(",")[0]+"\t"+p.split(",")[0]);
                				}
                				
                				result.set(r+"");
                				context.write(k, result);
                			}
                		}
                	}
            }
                buffer.clear();
        }
    }

        public static Double simcalculator(String record1,String record2){
        	
        
            String[] rd1 = record1.split("\t");
            String[] rd2 = record2.split("\t");
            
            double result  = 0.0;
            double intersection = 0.0;
            double union = rd1.length + rd2.length;
            
            
            
            
            for(String s : rd1){
				for(String j : rd2){
                    if(s.equals(j)){
                    	union = union - 1.0;
                    	intersection = intersection + 1.0;
                    }
                }
            }
            
            result = intersection/union;
            return result;

    }
        
        
    public static class RDMapper extends Mapper<Object, Text, Pair, Text> {
    	 Pair p = new Pair();
    	 Text result = new Text();
    	 int count = 0;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
        	String[] k = value.toString().split("\t");
        	
        	p.setPair(k[0], k[1]);
        	result.set(k[2]);
        	context.write(p, result);
        	
        }
    }
    public static class S3Partitioner extends
    Partitioner < Pair, Text >
    {
       @Override
       public int getPartition(Pair key, Text value, int numReduceTasks)
      
       {
         return key.getId().hashCode()% numReduceTasks;
       }

	
    }



    public static class RDReducer extends Reducer<Pair, Text, Text, Text> {
    	 Text val = new Text();
    	 Text k  = new Text();
        public void reduce(Pair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	for(Text v : values){
        		val.set(v);
        		break;
        	}
        	k.set("("+key.getId()+","+key.getVals()+")");
        	
        	context.write(k, val);
        }
    }


    public  int run(String[] args) throws Exception {        

        IN = args[0];

        OUT = args[1];

        //String SIMILARITY_THRESHOLD = args[2];

        int REDUCER_NUM = Integer.parseInt(args[3]);

        String input = IN;

        String output = OUT ;

        Configuration conf = new Configuration();
        conf.set("threshold",args[2]);

        Job stage2 = Job.getInstance(conf,"stage2");
        stage2.setJarByClass(SetSimJoin.class);
        
        FileInputFormat.addInputPath(stage2, new Path(input));
        FileOutputFormat.setOutputPath(stage2, new Path(output));
        
        stage2.setMapperClass(FSMapper.class);
        stage2.setMapOutputKeyClass(Text.class);
        stage2.setMapOutputValueClass(Text.class);
        
        stage2.setPartitionerClass(S2Partitioner.class);

        stage2.setReducerClass(FSReducer.class);
        
        stage2.setNumReduceTasks(REDUCER_NUM);

        stage2.setOutputKeyClass(Text.class);
        stage2.setOutputValueClass(Text.class);
       
        stage2.waitForCompletion(true);
        
        
        input = output;
        output = OUT + "/stage3";
        
     
        Configuration conf2 = new Configuration();
        Job stage3 = Job.getInstance(conf2,"stage3");

        FileInputFormat.addInputPath(stage3, new Path(input));
        FileOutputFormat.setOutputPath(stage3, new Path(output));
       
        

        stage3.setMapperClass(RDMapper.class);
        stage3.setMapOutputKeyClass(Pair.class);
        stage3.setMapOutputValueClass(Text.class);
        
        stage3.setPartitionerClass(S3Partitioner.class);

        stage3.setReducerClass(RDReducer.class);
        stage3.setNumReduceTasks(REDUCER_NUM);

        
        stage3.setOutputKeyClass(Text.class);
        stage3.setOutputValueClass(Text.class);

          
        
        System.exit(stage3.waitForCompletion(true) ? 0: 1);

        return 0;
    }
    public static void main(String ar[]) throws Exception{
    	int res = ToolRunner.run(new Configuration(),  new SetSimJoin(), ar);
    	System.exit(0);
    }

}

