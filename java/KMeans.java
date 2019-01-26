import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.List;
import java.util.Enumeration;
import java.net.URI; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class KMeans {
    
    public static double[][] digits = new double[10][785];
    public static int num_clusters = 10;
    public static int dimension = 784;
    


    public static class AvgMapper extends Mapper<Object,Text,Text,Text> {

    	@Override
    	public void map (Object key,Text line, Context context)
    	{
            String f = line.toString();
            String[] line_split = f.split(",");
            double label = Double.parseDouble(line_split[0]);
            double[] data = new double[dimension]; 
            for(int i =0; i<dimension; i++)
            {
                 data[i] = Double.parseDouble(line_split[i+1]);    
            }
            //for(int i =0; i<dimension; i++)
            //{
            //System.out.println(data[i]);
            //} 
            int count = 0;
            double min_dist = Double.MAX_VALUE; 
            double eu_dist = Double.MAX_VALUE ;
            double min_index = Double.MAX_VALUE;
        for(int i=0;i<num_clusters;i++)
        {
            double sum =0;
            for(int j=1; j<dimension;j++)
            {
                sum += Math.pow((data[j]-digits[i][j]),2);
                
            }
            eu_dist = Math.sqrt(sum);
            if (eu_dist < min_dist)
                {
                    min_dist = eu_dist; 
                    min_index = count; 
                }
                count = count+1;
          
        }
        System.out.println(min_index + " " + label);
        try
		{
    		context.write(new Text(String.valueOf(label)), new Text(String.valueOf(min_index)));
    		}catch(Exception e)
		{
			System.out.println("Exception");
		}
		}

    	}
    

    

    public static class AvgReducer extends Reducer<Text,Text,Text,Object> {
	@Override 
    	public void reduce (Text c, Iterable<Text> points, Context context)
    	{

            String c1 = c.toString();
            double c2 = Double.parseDouble(c1);
            int count = 0; 
    		double accuracy = 0.0;
    		for (Text p : points)
    		{   
                String a = p.toString();
                double b = Double.parseDouble(a);
                count+=1;
                if(b==c2)
                {
                    accuracy+=1;
                }
    		}
    		accuracy = (accuracy*100)/count;

    		try {	
    		context.write(new Text(String.valueOf(accuracy)), NullWritable.get());
 	  	}catch(Exception e)
{
		System.out.println("Exception");
}
	
}
    	}
    

    public static void main ( String[] args ) throws Exception {

        BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[1]))));
            String x;   
            
        while((x = r.readLine())!= null) {

            String[] y = x.split(",");
            double label = Double.parseDouble(y[0]);
            digits[(int)label][0] += 1.0; 
            for(int i= 1; i< y.length; i++){

                digits[(int)label][i] += Double.parseDouble(y[i]);
            }
        }
        for(int i=0; i<num_clusters; i++)
        {
            for(int j=1; j<dimension; j++)
            {
                digits[i][j] = digits[i][j]/digits[i][0];

            }
            r.close();
        }

    	Job job = Job.getInstance();
        job.setJobName("KmeansJob");
        
        job.setJarByClass(KMeans.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(AvgMapper.class);
        
        job.setReducerClass(AvgReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        
        //job.addCacheFile(new URI(args[1]));

        job.waitForCompletion(true);
   }
}


