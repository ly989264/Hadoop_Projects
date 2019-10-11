import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class TableJoin {

    public static class MyMap extends MapWritable {
        @Override
        public String toString() {
            String ret = "";
            for (Map.Entry e : this.entrySet()) {
                ret += e.getKey().toString() + ":" + e.getValue().toString() + " ";
            }
            return ret;
        }


    }

    public static class StudentMapper extends Mapper<LongWritable, Text, Text, MyMap> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            MyMap val = new MyMap();
            val.put(new Text("studentId"), new Text(parts[0]));
            val.put(new Text("name"), new Text(parts[1]));
            context.write(new Text(parts[0]), val);
        }
    }

    public static class MarkMapper extends Mapper<LongWritable, Text, Text, MyMap> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            MyMap val = new MyMap();
            val.put(new Text("assignment_1"), new IntWritable(Integer.parseInt(parts[1])));
            val.put(new Text("assignment_2"), new IntWritable(Integer.parseInt(parts[2])));
            context.write(new Text(parts[0]), val);
        }
    }

    public static class MyReducer extends Reducer<Text, MyMap, Text, MyMap> {
        @Override
        protected void reduce(Text key, Iterable<MyMap> values, Context context) throws IOException, InterruptedException {
            MyMap value = new MyMap();
            for (MyMap a : values) {
                value.putAll(a);
            }
            // if want to write the key-value pair
            // context.write(key, value);
            // if want to write the name-value pair
            context.write(new Text(value.get(new Text("name")).toString()), value);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Table Join");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyMap.class);

        // if two mappers' output format is the same
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyMap.class);
        // what if not same?

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MarkMapper.class);

        job.setReducerClass(MyReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}

// MapWritable does not have the toString() function that can properly display the key-value pairs
// solution: define a new class that inherits from the MapWritable class, but also overwrite the toString() method
