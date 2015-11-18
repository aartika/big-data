import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

/**
 * Created by rohit on 11/19/2015.
 */
public class FrequencyCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FrequencyCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if (args.length != 2) {
            System.err.println("Arguments : reviewsPath  count");
            return 1;
        }
        conf.set("reviewsPath", args[0]);
        conf.set("frequencyCountPath", args[1]);
        //conf.set("mapreduce.job.user.classpath.first", "true");

        return  frequencyCountjob(conf) ? 0 : 1;
    }



    private static boolean frequencyCountjob(Configuration conf) throws Exception {

        Job job = Job.getInstance(conf, "Frequency count");

        job.addFileToClassPath(new Path("/code/big-data-project-1.0-SNAPSHOT-jar-with-dependencies.jar"));
        Path outputDir = new Path(conf.get("frequencyCountPath"));
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, conf.get("reviewsPath"));
        TextOutputFormat.<Text, Text>setOutputPath(job, outputDir);

        job.setMapperClass(FrequencyCountMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(FrequencyCountReducer.class);

        return job.waitForCompletion(true);
    }
}


