import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class Assignment1_v2 {

    // custom Writable includes user and rating, used for FirstMapper output value
    public static class UserRatingWritable implements WritableComparable<UserRatingWritable> {

        private Text user;
        private IntWritable rating;  // rating for the first movie
        private IntWritable rating2;  // rating for the second movie

        public UserRatingWritable() {
            this.user = new Text("");
            this.rating = new IntWritable(-1);
            this.rating2 = new IntWritable(-1);
        }

        public UserRatingWritable(Text user, IntWritable rating, IntWritable rating2) {
            super();
            set(user, rating, rating2);
        }

        public Text getUser() {
            return user;
        }

        public void setUser(Text user) {
            this.user = user;
        }

        public IntWritable getRating() {
            return rating;
        }

        public void setRating(IntWritable rating) {
            this.rating = rating;
        }

        public IntWritable getRating2() {
            return rating2;
        }

        public void setRating2(IntWritable rating2) {
            this.rating2 = rating2;
        }

        public void set(Text user, IntWritable rating, IntWritable rating2) {
            this.user = user;
            this.rating = rating;
            this.rating2 = rating2;
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.user.readFields(dataInput);
            this.rating.readFields(dataInput);
            this.rating2.readFields(dataInput);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.user.write(dataOutput);
            this.rating.write(dataOutput);
            this.rating2.write(dataOutput);
        }

        @Override
        public int hashCode() {
            return this.user.hashCode() * 163 + this.rating.hashCode() + 31 + this.rating2.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof UserRatingWritable) {
                UserRatingWritable corre = (UserRatingWritable) obj;
                return this.user.equals(corre.getUser()) && this.rating.equals(corre.getRating()) && this.rating2.equals(corre.getRating2());
            }
            return false;
        }

        @Override
        public int compareTo(UserRatingWritable o) {
            int cmp = this.user.compareTo(o.getUser());
            if (cmp != 0) {
                return cmp;
            }
            cmp = this.rating.compareTo(o.getRating());
            if (cmp != 0) {
                return cmp;
            }
            return this.rating2.compareTo(o.getRating2());
        }

        @Override
        public String toString() {
            return this.user.toString() + ">" + this.rating.get() + ">" + this.rating2.get();
        }

    }

    // the result output UserRatingWritable class, override the toString method
    public static class ResUserRatingWritable extends UserRatingWritable {

        public ResUserRatingWritable() {
            super();
        }

        public ResUserRatingWritable(Text user, IntWritable rating, IntWritable rating2) {
            super(user, rating, rating2);
        }

        @Override
        public String toString() {
            return "(" + this.getUser().toString() + "," + this.getRating().toString() + "," + this.getRating2().toString() + ")";
        }


    }

    public static class MovieRatingWritable implements WritableComparable<MovieRatingWritable> {

        private Text movie;
        private IntWritable rating;

        public MovieRatingWritable() {
            this.movie = new Text();
            this.rating = new IntWritable();
        }

        public MovieRatingWritable(Text movie, IntWritable rating) {
            super();
            this.movie = movie;
            this.rating = rating;
        }

        public Text getMovie() {
            return movie;
        }

        public void setMovie(Text movie) {
            this.movie = movie;
        }

        public IntWritable getRating() {
            return rating;
        }

        public void setRating(IntWritable rating) {
            this.rating = rating;
        }

        @Override
        public int hashCode() {
            return this.movie.hashCode() * 163 + this.rating.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof MovieRatingWritable) {
                MovieRatingWritable corre = (MovieRatingWritable) obj;
                return this.movie.equals(((MovieRatingWritable) obj).getMovie()) && this.rating.equals(((MovieRatingWritable) obj).getRating());
            }
            return false;
        }

        @Override
        public int compareTo(MovieRatingWritable o) {
            int tmp = this.movie.compareTo(o.getMovie());
            if (tmp != 0) {
                return tmp;
            }
            return this.rating.compareTo(o.getRating());
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.movie.write(dataOutput);
            this.rating.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.movie.readFields(dataInput);
            this.rating.readFields(dataInput);
        }

        @Override
        public String toString() {
            return this.movie.toString() + "<" + this.rating.get();
        }
    }

    // first job key format
    public static class PairWritable implements WritableComparable<PairWritable> {

        private Text m1;
        private Text m2;

        public PairWritable() {
            this.m1 = new Text("");
            this.m2 = new Text("");
        }

        public PairWritable(Text m1, Text m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        public Text getM1() {
            return m1;
        }

        public void setM1(Text m1) {
            this.m1 = m1;
        }

        public Text getM2() {
            return m2;
        }

        public void setM2(Text m2) {
            this.m2 = m2;
        }

        @Override
        public int compareTo(PairWritable o) {
            if (this.m1.compareTo(o.getM1()) == 0 && this.m2.compareTo(o.getM2()) == 0) {
                return 0;
            } else if ((this.m1.compareTo(o.getM1()) == 0 && this.m2.compareTo(o.getM2()) > 0) || this.m1.compareTo(o.getM1()) > 0) {
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.m1.readFields(dataInput);
            this.m2.readFields(dataInput);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.m1.write(dataOutput);
            this.m2.write(dataOutput);
        }



        @Override
        public String toString() {
            return "" + this.m1 + "," + this.m2 + "";
        }
    }

    // result output key format, override the PairWriable method
    public static class ResPairWritable extends PairWritable {

        public ResPairWritable() {
            super();
        }

        public ResPairWritable(Text m1, Text m2) {
            super(m1, m2);
        }

        @Override
        public String toString() {
            return "(" + this.getM1().toString() + "," + this.getM2().toString() + ")";
        }


    }

    public static class FirstMapper extends Mapper<LongWritable, Text, Text, MovieRatingWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] contents = value.toString().split("::");
            MovieRatingWritable val = new MovieRatingWritable(new Text(contents[1]), new IntWritable(Integer.parseInt(contents[2])));
            context.write(new Text(contents[0]), val);
        }
    }

    public static class MRSC implements Comparable<MRSC> {

        private String movie;
        private int rate;

        public MRSC(String movie, int rate) {
            this.movie = movie;
            this.rate = rate;
        }

        public String getMovie() {
            return movie;
        }

        public void setMovie(String movie) {
            this.movie = movie;
        }

        public int getRate() {
            return rate;
        }

        public void setRate(int rate) {
            this.rate = rate;
        }

        @Override
        public int compareTo(MRSC o) {
            return this.movie.compareTo(o.getMovie());
        }
    }

    public static void delete_file(File file) throws IOException {
        if (file.isDirectory()) {
            if (file.list().length == 0) {
                file.delete();
            } else {
                String files[] = file.list();
                for (String tmp : files) {
                    File fileD = new File(file, tmp);
                    delete_file(fileD);
                }
                if (file.list().length == 0) {
                    file.delete();
                }
            }
        } else {
            file.delete();
        }
    }

    public static void delete_directory(String directoryname) {
        File directory = new File(directoryname);
        if (directory.exists()) {
            try {
                delete_file(directory);
            } catch (IOException e) {
            }
        }
    }

    // change here, MyArrayWritable should accept ResUserRatingWritable objects
    public static class MyArrayWritable extends ArrayWritable {

        public MyArrayWritable() {
            super(ResUserRatingWritable.class);
        }

        public MyArrayWritable(ResUserRatingWritable[] arr) {
            this();
            this.set(arr);
        }

//        public MyArrayWritable(ArrayList<UserRatingWritable> arr) {
//            this();
//            this.set(arr);
//        }

        @Override
        public String toString() {
            String[] strings = super.toStrings();
            String res = "[";
            int cnt = 0;
            for (String each : strings) {
                if (cnt != 0) {
                    res += ",";
                }
                res += each;
                cnt++;
            }
            res += "]";
            return res;
        }
    }

    public static class FirstReducer extends Reducer<Text, MovieRatingWritable, PairWritable, UserRatingWritable> {

        @Override
        protected void reduce(Text key, Iterable<MovieRatingWritable> values, Context context) throws IOException, InterruptedException {
            // delete file
//            delete_file(new File("r.dat"));
            // need to sort first
            ArrayList<MRSC> mrsc_arr = new ArrayList<>();
            // iterate
//            ArrayList<String> movie_arr = new ArrayList<>();
//            ArrayList<Integer> rating_arr = new ArrayList<>();
            for (MovieRatingWritable each : values) {
//                movie_arr.add(each.getMovie().toString());
//                rating_arr.add(each.getRating().get());
                mrsc_arr.add(new MRSC(each.getMovie().toString(), each.getRating().get()));
            }
            Collections.sort(mrsc_arr);
            int arr_length = mrsc_arr.size();
            for (int index_i = 0; index_i < arr_length; index_i++) {
                for (int index_j = index_i; index_j < arr_length; index_j++) {
                    if (mrsc_arr.get(index_i).getMovie().equals(mrsc_arr.get(index_j).getMovie())) {
                        continue;
                    }
                    PairWritable resp = new PairWritable(new Text(mrsc_arr.get(index_i).getMovie()), new Text(mrsc_arr.get(index_j).getMovie()));
                    UserRatingWritable resu = new UserRatingWritable(key, new IntWritable(mrsc_arr.get(index_i).getRate()), new IntWritable(mrsc_arr.get(index_j).getRate()));
                    context.write(resp, resu);
                }
            }
        }
    }

    public static class SecondMapper extends Mapper<LongWritable, Text, ResPairWritable, ResUserRatingWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s_arr_1 = value.toString().split("\\s");
            String[] s_arr_2 = s_arr_1[0].split(",");
            ResPairWritable p_res = new ResPairWritable(new Text(s_arr_2[0]), new Text(s_arr_2[1]));
            String[] s_arr_3 = s_arr_1[1].split(">");
            ResUserRatingWritable ur_res = new ResUserRatingWritable(new Text(s_arr_3[0]), new IntWritable(Integer.parseInt(s_arr_3[1])), new IntWritable(Integer.parseInt(s_arr_3[2])));
            context.write(p_res, ur_res);
        }
    }

    public static class SecondReducer extends Reducer<ResPairWritable, ResUserRatingWritable, ResPairWritable, MyArrayWritable> {

        @Override
        protected void reduce(ResPairWritable key, Iterable<ResUserRatingWritable> values, Context context) throws IOException, InterruptedException {
            delete_directory("output");
            ArrayList<ResUserRatingWritable> urarr = new ArrayList<>();
            for (ResUserRatingWritable each : values) {
                if (urarr.contains(each)) {
                    System.out.println("catch");
                    continue;
                }
                urarr.add(new ResUserRatingWritable(new Text(each.getUser().toString()), new IntWritable(each.getRating().get()), new IntWritable(each.getRating2().get())));
            }
            ResUserRatingWritable[] ur_arr_arr = new ResUserRatingWritable[urarr.size()];
            ur_arr_arr = urarr.toArray(ur_arr_arr);
            MyArrayWritable arr_res = new MyArrayWritable(ur_arr_arr);
            context.write(key, arr_res);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Job1");

        job1.setOutputKeyClass(PairWritable.class);
        job1.setOutputValueClass(UserRatingWritable.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(MovieRatingWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.setMapperClass(FirstMapper.class);
        job1.setReducerClass(FirstReducer.class);

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Job2");

        job2.setMapOutputKeyClass(ResPairWritable.class);
        job2.setMapOutputValueClass(ResUserRatingWritable.class);
//        job2.setMapOutputValueClass(URArrayWritable.class);

        job2.setOutputKeyClass(ResPairWritable.class);
        job2.setOutputValueClass(MyArrayWritable.class);
//        job2.setOutputValueClass(URArrayWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);

        job2.waitForCompletion(true);



    }
}

