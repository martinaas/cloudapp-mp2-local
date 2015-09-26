import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Links Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopPopularLinks.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Links Popularity");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(PopularityMap.class);
        jobB.setReducerClass(TopLinksReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopPopularLinks.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer stringTokenizer1 = new StringTokenizer(line, ":");
            String pageId = stringTokenizer1.nextToken();
            context.write(new IntWritable(Integer.valueOf(pageId)), new IntWritable(0));
            StringTokenizer stringTokenizer2 = new StringTokenizer(stringTokenizer1.nextToken().trim(), " ");
            while (stringTokenizer2.hasMoreTokens()) {
                String nextToken = stringTokenizer2.nextToken().trim();
                context.write(new IntWritable(Integer.valueOf(nextToken)), new IntWritable(1));
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PopularityMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        List<String> league;
        private Map<Integer, Integer> popularityMap =
                new HashMap<Integer, Integer>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            this.league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer popularity = Integer.parseInt(value.toString());
            Integer pageId = Integer.parseInt(key.toString());
            popularityMap.put(pageId, popularity);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Integer> entry : popularityMap.entrySet())
            {
                if (league.contains(String.valueOf(entry.getKey()))) {
                    Integer[] integers = {entry.getKey(),
                            entry.getValue()};
                    IntArrayWritable val = new
                            IntArrayWritable(integers);
                    context.write(NullWritable.get(), val);
                }
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        List<String> league;
        private Map<Integer, Integer> tempMap =
                new HashMap<Integer, Integer>();
        private Map<Integer, Integer> ranksMap =
                new HashMap<Integer, Integer>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            this.league = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (IntArrayWritable val: values) {
                IntWritable[] pair = (IntWritable[]) val.toArray();
                Integer pageId = Integer.parseInt(pair[0].toString());
                Integer count =
                        Integer.parseInt(pair[1].toString());
                tempMap.put(pageId, count);
            }

            for (String leaguePageId : league) {
                if (tempMap.containsKey(Integer.parseInt(leaguePageId)))
                {
                    Integer leaguePagePopularity = tempMap.get(leaguePageId);
                    int counter = 0;
                    if (leaguePagePopularity != null) {
                        for (Map.Entry<Integer, Integer> entry : tempMap.entrySet()) {
                            if (leaguePagePopularity.intValue() > entry.getValue())
                                counter++;
                        }
                    }
                    ranksMap.put(Integer.parseInt(leaguePageId), counter);
                }
            }

            for (Map.Entry<Integer, Integer> entry : ranksMap.entrySet()) {
                IntWritable id = new IntWritable(entry.getKey());
                IntWritable value = new IntWritable(entry.getValue());
                context.write(id, value);
            }
        }
    }
}
