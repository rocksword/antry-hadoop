package com.an.antry.hadoop.wfr;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WholeFileTest {
    private static final Log logger = LogFactory.getLog(WholeFileTest.class);

    public static class mapper extends Mapper<Text, BytesWritable, Text, Text> {
        @Override
        protected void map(Text key, BytesWritable value, Context context) {
            logger.info("mapper key: " + key.toString());
            logger.info("value: " + new Text(value.getBytes()).toString().length());
            try {
                context.write(key, new Text(value.getBytes()));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) {
            logger.info("reducer key: " + key.toString());
            for (Text t : values) {
                try {
                    logger.info("text: " + t.toString().length());
                    context.write(key, t);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SussAnalyzerMain <input path> <output path>");
            System.exit(-1);
        }
        String inputPath = args[0].trim();
        String outputPath = args[1].trim();
        logger.info("inputPath: " + inputPath + ", outputPath: " + outputPath);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WholeFileTest");
        job.setJarByClass(WholeFileTest.class);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setInputFormatClass(WholeFileInputformat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        logger.info("Job finished: " + (job.waitForCompletion(true) ? "true" : "false"));
    }
}

class WholeFileInputformat extends FileInputFormat<Text, BytesWritable> {
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new WholeFileRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}

class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
    private static final Log logger = LogFactory.getLog(WholeFileRecordReader.class);
    private FileSplit fileSplit;
    private FSDataInputStream fis;

    private Text key = null;
    private BytesWritable value = null;

    private boolean processed = false;

    @Override
    public void close() throws IOException {
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? fileSplit.getLength() : 0;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext tacontext) throws IOException,
            InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        Configuration job = tacontext.getConfiguration();
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(job);
        fis = fs.open(file);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            logger.info("new Text");
            key = new Text();
        }
        if (value == null) {
            logger.info("new BytesWritable");
            value = new BytesWritable();
        }
        if (!processed) {
            byte[] content = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            logger.info("filename: " + file.getName());

            key.set(file.getName());
            try {
                IOUtils.readFully(fis, content, 0, content.length);
                value.set(new BytesWritable(content));
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(fis);
            }
            processed = true;
            // Indicate that have not finished, will return next key value pair.
            return true;
        }
        // return false, indicate that have finished all key values.
        return false;
    }
}
