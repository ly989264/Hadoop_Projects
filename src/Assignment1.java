//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//
//public class Assignment1 {
//
//    // custom Writable includes user and rating, used for FirstMapper output value
//    public static class UserRatingWritable implements WritableComparable<UserRatingWritable> {
//
//        private Text user;
//        private IntWritable rating;
//
//        public UserRatingWritable() {
//            this.user = new Text("");
//            this.rating = new IntWritable(-1);
//        }
//
//        public UserRatingWritable(Text user, IntWritable rating) {
//            super();
//            this.user = user;
//            this.rating = rating;
//        }
//
//        public Text getUser() {
//            return user;
//        }
//
//        public void setUser(Text user) {
//            this.user = user;
//        }
//
//        public IntWritable getRating() {
//            return rating;
//        }
//
//        public void setRating(IntWritable rating) {
//            this.rating = rating;
//        }
//
//        public void set(Text user, IntWritable rating) {
//            this.user = user;
//            this.rating = rating;
//        }
//
//        @Override
//        public void readFields(DataInput dataInput) throws IOException {
//            this.user.readFields(dataInput);
//            this.rating.readFields(dataInput);
//        }
//
//        @Override
//        public void write(DataOutput dataOutput) throws IOException {
//            this.user.write(dataOutput);
//            this.rating.write(dataOutput);
//        }
//
//        @Override
//        public int hashCode() {
//            return this.user.hashCode() * 163 + this.rating.hashCode();
//        }
//
//        @Override
//        public boolean equals(Object obj) {
//            if (obj instanceof UserRatingWritable) {
//                UserRatingWritable corre = (UserRatingWritable) obj;
//                return this.user.equals(corre.getUser()) && this.rating.equals(corre.getRating());
//            }
//            return false;
//        }
//
//        @Override
//        public int compareTo(UserRatingWritable o) {
//            int cmp = this.user.compareTo(o.getUser());
//            if (cmp != 0) {
//                return cmp;
//            }
//            return this.rating.compareTo(o.getRating());
//        }
//
//        @Override
//        public String toString() {
//            return this.user.toString() + "->" + this.rating.get();
//        }
//
//    }
//
//    public static class PairWritable implements WritableComparable<PairWritable> {
//
//        private Text m1;
//        private Text m2;
//
//        public PairWritable(Text m1, Text m2) {
//            this.m1 = m1;
//            this.m2 = m2;
//        }
//
//        public Text getM1() {
//            return m1;
//        }
//
//        public void setM1(Text m1) {
//            this.m1 = m1;
//        }
//
//        public Text getM2() {
//            return m2;
//        }
//
//        public void setM2(Text m2) {
//            this.m2 = m2;
//        }
//
//        @Override
//        public int compareTo(PairWritable o) {
//            if (this.m1.compareTo(o.getM1()) == 0 && this.m2.compareTo(o.getM2()) == 0) {
//                return 0;
//            } else if ((this.m1.compareTo(o.getM1()) == 0 && this.m2.compareTo(o.getM2()) > 0) || this.m1.compareTo(o.getM1()) > 0) {
//                return 1;
//            } else {
//                return -1;
//            }
//        }
//
//        @Override
//        public void readFields(DataInput dataInput) throws IOException {
//            this.m1.readFields(dataInput);
//            this.m2.readFields(dataInput);
//        }
//
//        @Override
//        public void write(DataOutput dataOutput) throws IOException {
//            this.m1.write(dataOutput);
//            this.m2.write(dataOutput);
//        }
//
//
//
//        @Override
//        public String toString() {
//            return "(" + this.m1 + "," + this.m2 + ")";
//        }
//    }
//
//    public static class URArrayWritable extends ArrayWritable {
//
//
//        public URArrayWritable() {
//            super(UserRatingWritable.class);
//        }
//
//        public URArrayWritable(UserRatingWritable[] strings) {
//            super(UserRatingWritable.class);
//            this.set(strings);
//        }
//
//        @Override
//        public void set(Writable[] values) {
//            super.set(values);
//        }
//
//        @Override
//        public void readFields(DataInput in) throws IOException {
//            super.readFields(in);
//        }
//
//        @Override
//        public String toString() {
//            // need to implement this
//            String[] strings = super.toStrings();
//            String res = "";
//            for (String each : strings) {
//                res += each;
//                res += "    ";
//            }
////            System.out.println(res);
//            return res;
//        }
//    }
//
//    public static class ContentWritable implements Writable {
//
//        private Text movie;
//        private URArrayWritable pairs;
//
//        public ContentWritable() {
//            this.movie = new Text();
//            this.pairs = new URArrayWritable();
//        }
//
//        public ContentWritable(Text movie, URArrayWritable pairs) {
//            super();
//            this.movie = movie;
//            this.pairs = pairs;
//        }
//
//        public Text getMovie() {
//            return movie;
//        }
//
//        public void setMovie(Text movie) {
//            this.movie = movie;
//        }
//
//        public URArrayWritable getPairs() {
//            return pairs;
//        }
//
//        public void setPairs(URArrayWritable pairs) {
//            this.pairs = pairs;
//        }
//
//        public void set(Text movie, URArrayWritable pairs) {
//            this.movie = movie;
//            this.pairs = pairs;
//        }
//
//        @Override
//        public int hashCode() {
//            return this.movie.hashCode() * 163 + this.pairs.hashCode();
//        }
//
//        @Override
//        public void readFields(DataInput dataInput) throws IOException {
//            this.movie.readFields(dataInput);
//            this.pairs.readFields(dataInput);  // something wrong with this line
//        }
//
//        @Override
//        public void write(DataOutput dataOutput) throws IOException {
//            this.movie.write(dataOutput);
//            this.pairs.write(dataOutput);
//        }
//
//        @Override
//        public String toString() {
//            return this.movie.toString() + "->" + this.pairs.toString();
//        }
//    }
//
//    public static class FirstMapper extends Mapper<LongWritable, Text, Text, UserRatingWritable> {
//
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] contents = value.toString().split("::");
//            UserRatingWritable val = new UserRatingWritable(new Text(contents[0]), new IntWritable(Integer.parseInt(contents[2])));
//            context.write(new Text(contents[1]), val);
//        }
//    }
//
//
//    // TODO: something wrong with arraywritable, need to fix that!
//
//    public static class FirstReducer extends Reducer<Text, UserRatingWritable, Text, Text> {
//
//        @Override
//        protected void reduce(Text key, Iterable<UserRatingWritable> values, Context context) throws IOException, InterruptedException {
//            String res = "";
//            int cnt = 0;
//            for (UserRatingWritable each : values) {
//                if (cnt != 0) {
//                    res += "::";
//                }
//                res += each.toString();
//                cnt++;
//            }
//            context.write(key, new Text(res));
//        }
//    }
//
//    public static class SecondMapper extends Mapper<LongWritable, Text, NullWritable, ContentWritable> {
//
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
////            System.out.println(value.toString());
//            String[] string_list_1 = value.toString().split("\\s");
////            System.out.println(string_list_1.length);
//            String movie_id = string_list_1[0];
//            String[] string_list_2 = string_list_1[1].split("::");
//            UserRatingWritable[] urwl = new UserRatingWritable[string_list_2.length];
//            for (int i = 0; i < string_list_2.length; i++) {
//                String[] string_list_3 = string_list_2[i].split("->");
//                urwl[i] = new UserRatingWritable(new Text(string_list_3[0]), new IntWritable(Integer.parseInt(string_list_3[1])));
//            }
////            System.out.println(urwl.length);
//            URArrayWritable maw = new URArrayWritable(urwl);
////            maw.set(urwl);
////            System.out.println(maw);
////            System.out.println("Done");
//            ContentWritable result = new ContentWritable(new Text(movie_id), maw);
//            context.write(NullWritable.get(), result);
////            System.out.println("Done");
//        }
//    }
//
//    // testing for the second mapper
//
//    public static class SecondReducer extends Reducer<NullWritable, ContentWritable, PairWritable, URArrayWritable> {
//
//        @Override
//        protected void reduce(NullWritable key, Iterable<ContentWritable> values, Context context) throws IOException, InterruptedException {
//            Map<String, UserRatingWritable[]> tmap = new HashMap<>();
//            for (ContentWritable each : values) {
//                tmap.put(each.getMovie().toString(), (UserRatingWritable[])each.getPairs().toArray());
//            }
//            for (String a : tmap.keySet()) {
//                HashSet<UserRatingWritable> set1 = new HashSet<>();
//                set1.addAll(Arrays.asList(tmap.get(a)));
//                for (String b : tmap.keySet()) {
//                    if (!a.equals(b)) {
//                        HashSet<UserRatingWritable> set2 = new HashSet<>();
//                        set2.addAll(Arrays.asList(tmap.get(b)));
//                        set2.retainAll(set1);
//                        UserRatingWritable[] tmp = new UserRatingWritable[set2.toArray().length];
//                        tmp = set2.toArray(tmp);
//                        // TODO: note a b and b a
//                        context.write(new PairWritable(new Text(a), new Text(b)), new URArrayWritable(tmp));
//                    }
//                }
//            }
//        }
//    }
//
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//
//        Job job1 = Job.getInstance(conf, "Job1");
//
//        job1.setOutputKeyClass(Text.class);
//        job1.setOutputValueClass(Text.class);
//
//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(UserRatingWritable.class);
//
//        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
//
//        job1.setMapperClass(FirstMapper.class);
//        job1.setReducerClass(FirstReducer.class);
//
//        job1.waitForCompletion(true);
//
//        Job job2 = Job.getInstance(conf, "Job2");
//
//        job2.setMapOutputKeyClass(NullWritable.class);
//        job2.setMapOutputValueClass(ContentWritable.class);
////        job2.setMapOutputValueClass(URArrayWritable.class);
//
//        job2.setOutputKeyClass(PairWritable.class);
//        job2.setOutputValueClass(URArrayWritable.class);
////        job2.setOutputValueClass(URArrayWritable.class);
//
//        FileInputFormat.addInputPath(job2, new Path(args[2]));
//        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
//
//        job2.setMapperClass(SecondMapper.class);
//        job2.setReducerClass(SecondReducer.class);
//
//        job2.waitForCompletion(true);
//
//
//
//    }
//}
//
//// use ArrayWritable is fine, if both Mapper and Reducer return the key-value pair with value of ArrayWritable
//// but in this case, if running two jobs together, the second job cannot pass to the reduce phase (automatically terminated)
//
//// if use ContentWritable, then it seems that there is something wrong with the readFields method of the ContentWritable
//// especially, it seems that this error is caused by the ArrayWritable...
//// solution: default constructor need to create the object!
//
//// Why?
