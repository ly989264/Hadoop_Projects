import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableJoin_v2 {

    public static class StdMarkWritable implements Writable {
        private Text name;
        private IntWritable assignment_1;
        private IntWritable assignment_2;

        public StdMarkWritable() {
            this.name = new Text("");
            this.assignment_1 = new IntWritable(-1);
            this.assignment_2 = new IntWritable(-1);
        }

        public StdMarkWritable(Text name, IntWritable assignment_1, IntWritable assignment_2) {
            super();
            this.name = name;
            this.assignment_1 = assignment_1;
            this.assignment_2 = assignment_2;
        }

        public Text getName() {
            return name;
        }

        public void setName(Text name) {
            this.name = name;
        }

        public IntWritable getAssignment_1() {
            return assignment_1;
        }

        public void setAssignment_1(IntWritable assignment_1) {
            this.assignment_1 = assignment_1;
        }

        public IntWritable getAssignment_2() {
            return assignment_2;
        }

        public void setAssignment_2(IntWritable assignment_2) {
            this.assignment_2 = assignment_2;
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.name.readFields(dataInput);
            this.assignment_1.readFields(dataInput);
            this.assignment_2.readFields(dataInput);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.name.write(dataOutput);
            this.assignment_1.write(dataOutput);
            this.assignment_2.write(dataOutput);
        }

        @Override
        public String toString() {
            return this.name.toString() + " " + this.assignment_1.toString() + " " + this.assignment_2.toString();
        }
    }

    public static class StudentMapper extends Mapper<LongWritable, Text, Text, StdMarkWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            StdMarkWritable val = new StdMarkWritable();
            val.setName(new Text(parts[1]));
            context.write(new Text(parts[0]), val);
        }
    }

    public static class MarkMapper extends Mapper<LongWritable, Text, Text, StdMarkWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            StdMarkWritable val = new StdMarkWritable();
            val.setAssignment_1(new IntWritable(Integer.parseInt(parts[1])));
            val.setAssignment_2(new IntWritable(Integer.parseInt(parts[2])));
            context.write(new Text(parts[0]), val);
        }
    }

    public static class MyReducer extends Reducer<Text, StdMarkWritable, Text, StdMarkWritable> {
        @Override
        protected void reduce(Text key, Iterable<StdMarkWritable> values, Context context) throws IOException, InterruptedException {
            StdMarkWritable value = new StdMarkWritable();
            for (StdMarkWritable a : values) {
                // check if the name is empty or not
                if (a.getName().toString().equals("")) {
                    // means that only have assignemnts records
//                    value.setAssignment_1(a.getAssignment_1());  // wrong
//                    value.setAssignment_2(a.getAssignment_2());  // wrong
                    value.setAssignment_1(new IntWritable(a.getAssignment_1().get()));  // correct
                    value.setAssignment_2(new IntWritable(a.getAssignment_2().get()));  // correct
                } else {
//                    value.setName(a.getName());  // wrong
                    value.setName(new Text(a.getName().toString()));  // correct
                }
                // ESSENTIAL: need to create a new object, rather than just passing the variable, because of the reference?
                // ESSENTIAL HERE
            }
            // if want to write the key-value pair
            // context.write(key, value);
            // if want to write the name-value pair
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Table Join");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StdMarkWritable.class);

        // if two mappers' output format is the same
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StdMarkWritable.class);
        // what if not same?

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MarkMapper.class);

        job.setReducerClass(MyReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}

// define own Writable class, and use it in the MapReduce
