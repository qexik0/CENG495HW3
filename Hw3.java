import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Hw3
{
    private static class TotalMap extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            var tokens = value.toString().split("\t", -1);
	    try
	    {
            	context.write(new Text("total"), new DoubleWritable(Double.parseDouble(tokens[14])));
	    }
	    catch (NumberFormatException e) {}
        }
    }

    public static class TotalReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            double res = 0;
            for (DoubleWritable val : values)
	    {
                res += val.get();
            }
            context.write(new Text("total"), new DoubleWritable(res));
        }
    }

    public static class AverageReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
            double res = 0;
	    int i = 0;
            for (DoubleWritable val : values)
	    {
                res += val.get();
		i++;
            }
            context.write(new Text("average"), new DoubleWritable(res / i));
        }
    }


    public static class EmploymentReduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
	    int i = 0;
            for (IntWritable val : values)
	    {
		i += val.get();
            }
            context.write(key, new IntWritable(i));
        }
    }

    private static class EmploymentMap extends Mapper<Object, Text, Text, IntWritable>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            var tokens = value.toString().split("\t", -1);
	    if (!tokens[9].isEmpty())
	    {
		context.write(new Text(tokens[9]), new IntWritable(1));
	    }
        }
    }

    private static class RatescoreMap extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            var tokens = value.toString().split("\t", -1);
	    var rating = tokens[1];
	    try
	    {
	        if (rating.equals("G") || rating.equals("PG") || rating.equals("PG-13") || rating.equals("R"))
	        {
		    context.write(new Text("ratescore"), new DoubleWritable(Double.parseDouble(tokens[6])));
	        }
	    }
	    catch (NumberFormatException e) {}
        }
    }
    public static class RatescoreReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
	    double sum = 0, i = 0;
            for (DoubleWritable val : values)
	    {
		sum += val.get();
		i++;
            }
            context.write(key, new DoubleWritable(sum / i));
        }
    }

    private static class GenrescoreMap extends Mapper<Object, Text, Text, DoubleWritable>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            var tokens = value.toString().split("\t", -1);
	    var genre = tokens[2];
	    try
	    {
		    if (!genre.isEmpty())
		    {
			    context.write(new Text(genre), new DoubleWritable(Double.parseDouble(tokens[5])));
		    }
	    }
	    catch (NumberFormatException e) {}
        }
    }
    public static class GenrescoreReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
        {
	    double sum = 0, i = 0;
            for (DoubleWritable val : values)
	    {
		sum += val.get();
		i++;
            }
	    if (i > 9)
	    {
		    context.write(key, new DoubleWritable(sum / i));
	    }
        }
    }

    public static void main(String[] args) throws Exception 
    {
	var configuration = new Configuration();
        if (args[0].equals("total"))
        {
            var job = Job.getInstance(configuration, "total");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(TotalMap.class);
            job.setCombinerClass(TotalReduce.class);
            job.setReducerClass(TotalReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
	else if (args[0].equals("average"))
	{
            var job = Job.getInstance(configuration, "average");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(TotalMap.class);
            job.setCombinerClass(AverageReduce.class);
            job.setReducerClass(AverageReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	else if (args[0].equals("employment"))
	{
            var job = Job.getInstance(configuration, "employment");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(EmploymentMap.class);
            job.setCombinerClass(EmploymentReduce.class);
            job.setReducerClass(EmploymentReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	else if (args[0].equals("ratescore"))
	{
            var job = Job.getInstance(configuration, "ratescore");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(RatescoreMap.class);
            job.setCombinerClass(RatescoreReduce.class);
            job.setReducerClass(RatescoreReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	else if (args[0].equals("genrescore"))
	{
            var job = Job.getInstance(configuration, "genrescore");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(GenrescoreMap.class);
            job.setReducerClass(GenrescoreReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
    }
}
