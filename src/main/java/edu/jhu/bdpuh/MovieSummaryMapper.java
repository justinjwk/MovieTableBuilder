package edu.jhu.bdpuh;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieSummaryMapper extends TableMapper<Text, LongWritable> {
        private final LongWritable ONE = new LongWritable(1);
        private Text text = new Text();

        public void map(ImmutableBytesWritable row, Result value, Mapper.Context context) throws IOException, InterruptedException {
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
