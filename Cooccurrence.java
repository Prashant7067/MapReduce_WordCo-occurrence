import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

public class CooccurrenceCount {

    public static class CooccurrenceMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private Text wordPair = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] words = new String[itr.countTokens()];
            int i = 0;
            while (itr.hasMoreTokens()) {
                words[i] = itr.nextToken();
                i++;
            }
            for (int j = 0; j < words.length; j++) {
                for (int k = j + 1; k < words.length; k++) {
                    if (!words[j].equals(words[k])) {
                        wordPair.set(words[j] + " " + words[k]);
                        context.write(wordPair, one);
                    }
                }
            }
        }
    }

    public static class CooccurrenceReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "co-occurrence count");
        job.setJarByClass(CooccurrenceCount.class);
        job.setMapperClass(CooccurrenceMapper.class);
        job.setCombinerClass(CooccurrenceReducer.class);
        job.setReducerClass(CooccurrenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
