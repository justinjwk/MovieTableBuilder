package edu.jhu.bdpuh;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MovieSummaryJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        if(strings.length < 2) {
            System.out.printf("%s <movie-table> <outputPath>\n", this.getClass().getSimpleName());
            return 1;
        }
        TableName inputTable = TableName.valueOf(strings[0]);
        Path outputPath = new Path(strings[1]);
        Configuration config = HBaseConfiguration.create();

        Job job = Job.getInstance(config,"AnalyzeMovies");
        job.setJarByClass(MovieSummaryJob.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                inputTable.getNameAsString(),      // input table
                scan,             // Scan instance to control CF and attribute selection
                MovieSummaryMapper.class,   // mapper class
                Text.class,             // mapper output key
                LongWritable.class,             // mapper output value
                job);
        job.setReducerClass(MovieSummaryReducer.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        return 0;
    }

    static public void main(String[] args) throws Exception {

        int res = ToolRunner.run(new MovieSummaryJob(), args);
        System.exit(res);
    }
}
