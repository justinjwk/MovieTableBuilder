package edu.jhu.bdpuh;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MovieSummaryReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable value = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (LongWritable val : values) {
            i += val.get();
        }
        value.set(i);
        context.write(key, value);
    }

}
