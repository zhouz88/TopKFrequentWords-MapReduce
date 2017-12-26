
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.*;
import java.util.PriorityQueue;

public class TopKFrequentWords {

    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void setup(Context context) throws IOException{

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String s = value.toString().trim().toLowerCase();
             s = s.replaceAll("[^a-z]", " ");

             String[] words = s.split("\\s+");

             if (words.length == 0)
                 throw new RuntimeException();

             for (String k : words)
                 context.write(new Text(k), new IntWritable(1));
        }
    }

    public static class TopKReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> map = new HashMap<String, Integer>();
        private int K;

        protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            K = Integer.parseInt(conf.get("ValueOfK","10"));
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }

            map.put(key.toString(), sum);
        }
        //only once clean up and setup by mapreduce!
        protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            PriorityQueue<Map.Entry<String, Integer>> pq = new PriorityQueue<Map.Entry<String, Integer>>(new Comparator<Map.Entry<String, Integer>>(){
                public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
                    return a.getValue() == b.getValue() ? b.getKey().compareTo(a.getKey()) : a.getValue() - b.getValue();
                }
            });

            int cnt = 0;
            for (Map.Entry<String, Integer> e : map.entrySet()) {
                if (cnt < K) {
                    pq.add(e);
                    cnt++;
                } else {
                    pq.add(e);
                    pq.poll();
                }
            }
            List<Map.Entry<String, Integer>> q = new ArrayList<>();

            while (!pq.isEmpty())
                q.add(pq.poll());

            Collections.reverse(q);

            for (Map.Entry<String, Integer> e : q)
                context.write(new Text(e.getKey()), new IntWritable(e.getValue()));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TopKFrequentWords.class);
        conf.set("ValueOfK", args[2]);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(TopKReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}