package com.an.antry.hadoop.hia;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Listing 6.1 A MapClass with Counters to count the number of missing values
public class MyJobCounter extends Configured implements Tool {
    public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        static enum ClaimsCounters {
            MISSING, QUOTED
        };

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String fields[] = value.toString().split(",", -20);
            String country = fields[4];
            String numClaims = fields[8];
            if (numClaims.length() == 0) {
                reporter.incrCounter(ClaimsCounters.MISSING, 1);
            } else if (numClaims.startsWith("\"")) {
                reporter.incrCounter(ClaimsCounters.QUOTED, 1);
            } else {
                output.collect(new Text(country), new Text(numClaims + ",1"));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output,
                Reporter reporter) throws IOException {
            double sum = 0;
            int count = 0;
            while (values.hasNext()) {
                String fields[] = values.next().toString().split(",");
                sum += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }
            output.collect(key, new DoubleWritable(sum / count));
        }
    }

    public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            double sum = 0;
            int count = 0;
            while (values.hasNext()) {
                String fields[] = values.next().toString().split(",");
                sum += Double.parseDouble(fields[0]);
                count += Integer.parseInt(fields[1]);
            }
            output.collect(key, new Text(sum + "," + count));
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        JobConf job = new JobConf(conf, MyJob.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJobName("MyJobCounter");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combine.class);
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.set("key.value.separator.in.input.line", ",");
        JobClient.runJob(job);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);
        System.exit(res);
    }
}
