/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package edu.neu.csye6220.topnmovies;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Comparator;
/**
 *
 * @author tarun
 */
public class TopNMovies {
    // Mapper Class
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text movieId = new Text();
        private DoubleWritable rating = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                System.out.println("Mapper Input: " + value.toString());
                String line = value.toString().trim();
                if (line.startsWith("userId")) { // Skip header
                    return;
                }
                String[] fields = line.split(",");
                if (fields.length >= 3) { // Ensure sufficient columns
                    movieId.set(fields[1].trim()); // Movie ID is the second column
                    rating.set(Double.parseDouble(fields[2].trim())); // Rating is the third column
                    System.out.println("Mapper Output -> Key: " + movieId.toString() + ", Value: " + rating.get());
                    context.write(movieId, rating);
                }
            } catch (Exception e) {
                System.err.println("Error in Mapper: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    
    // Reducer Class
    public static class MovieReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private PriorityQueue<MovieRating> topMovies = new PriorityQueue<>(new Comparator<MovieRating>() {
            @Override
            public int compare(MovieRating o1, MovieRating o2) {
                return Double.compare(o1.rating, o2.rating);
            }
        });

        private static class MovieRating {
            String movieId;
            double rating;

            MovieRating(String movieId, double rating) {
                this.movieId = movieId;
                this.rating = rating;
            }
        }

        private int N = 10; // Default value for Top N

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            N = Integer.parseInt(conf.get("top.n", "10")); // Get the value of N from the configuration
            System.out.println("Reducer Setup: Top N = " + N);
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            try {
                System.out.println("Reducer Key: " + key.toString());
                double sum = 0;
                int count = 0;
                for (DoubleWritable value : values) {
                    System.out.println("Reducer Value for Key " + key.toString() + ": " + value.get());
                    sum += value.get();
                    count++;
                }
                if (count > 0) {
                    double avg = sum / count;
                    System.out.println("Reducer Calculated Average -> Movie: " + key.toString() + ", Average Rating: " + avg);

                    // Add to priority queue for Top N logic
                    topMovies.add(new MovieRating(key.toString(), avg));
                    if (topMovies.size() > N) {
                        topMovies.poll(); // Remove the movie with the lowest rating
                    }
                }
            } catch (Exception e) {
                System.err.println("Error in Reducer: " + e.getMessage());
                e.printStackTrace();
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("Reducer Cleanup: Writing Top N Movies to Context");
            while (!topMovies.isEmpty()) {
                MovieRating topMovie = topMovies.poll();
                context.write(new Text(topMovie.movieId), new DoubleWritable(topMovie.rating));
                System.out.println("Top Movie -> Movie: " + topMovie.movieId + ", Rating: " + topMovie.rating);
            }
        }
        
    }
    
    
    public static void main(String[] args)throws Exception {
        System.out.println("Starting Analysis for Top N movies!");
        Configuration conf = new Configuration();
        conf.set("top.n", args[2]); // Pass Top N as a command-line argument

        Job job = Job.getInstance(conf, "Top N Movies");
        job.setJarByClass(TopNMovies.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        System.out.println("Finished Analysis for Top N movies!");

    }
}
