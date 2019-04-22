package edu.jhu.bdpuh;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MovieSummaryMapper extends TableMapper<Text, LongWritable> {

        private final LongWritable ONE = new LongWritable(1);
        private Text text = new Text();

        int numOfMovies = 0;
        int numOfMoviesByReleaseMonth = 0;
        int numOfMoviesByReleaseYear = 0;
        int numOfMoviesByReviewLess10 = 0;
        int numOfMoviesByReview10To100 = 0;
        int numOfMoviesByReviewGreater100 = 0;

        Set<String> uniqueMovies = new HashSet<>();




        public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {

//            context.getCounter("MovieCounters", "Number of Movies");
//            context.getCounter("MovieCounters", "number of Movies by release month");
//            context.getCounter("MovieCounters", "Number of Movies by release year");
//            context.getCounter("MovieCounters", "Number of Movies by number of reviews less than 10");
//            context.getCounter("MovieCounters", "Number of Movies by number of reviews greater than 10 but less than 100");
//            context.getCounter("MovieCounters", "Number of Movies by number of reviews greater than 100");
//
//

            // column=item:Released, timestamp=1555894743748, value=01-Jan-1995
            String releaseDate[] = value.getColumnCells(Bytes.toBytes("item"), Bytes.toBytes("Realeased")).toString().split("=");
            String month = releaseDate[1];
            String year = releaseDate[2];

            // number of reviews == number of users
            int review = 0;





            for(String genre: MovieItem.GENRE) {
                context.getCounter("Genre-Tested", genre).increment(1);
                if(value.containsColumn(MovieTableBuilder.ITEM_CF.getBytes(), genre.getBytes())) {
                    context.getCounter("Genre-Match", genre).increment(1);
                    text.set("GENRE:"+genre);
                    context.write(text, ONE);
                }
            }
        }


}
