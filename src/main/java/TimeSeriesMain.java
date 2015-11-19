import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by rohit on 11/19/2015.
 */
public class TimeSeriesMain extends Configured implements Tool{

        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new Configuration(), new TimeSeriesMain(), args);
            System.exit(res);
        }

        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            if (args.length != 2) {
                System.err.println("Arguments : reviewsPath  reviewtimeseries");
                return 1;
            }
            conf.set("timeSeriesPath", args[0]);
            conf.set("reviewtimeseries", args[1]);
            //conf.set("mapreduce.job.user.classpath.first", "true");

            return  frequencyCountjob(conf) ? 0 : 1;
        }



        private static boolean frequencyCountjob(Configuration conf) throws Exception {

            Job job = Job.getInstance(conf, "Frequency count");
            job.setJarByClass(TimeSeriesMain.class);
            job.addFileToClassPath(new Path("/code/big-data-project-1.0-SNAPSHOT-jar-with-dependencies.jar"));
            Path outputDir = new Path(conf.get("reviewtimeseries"));
            FileSystem hdfs = FileSystem.get(conf);
            if (hdfs.exists(outputDir)) {
                hdfs.delete(outputDir, true);
            }
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job, conf.get("timeSeriesPath"));
            TextOutputFormat.<Text, Text>setOutputPath(job, outputDir);

            job.setMapperClass(TimeSeriesMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setReducerClass(TimeSeriesReducer.class);

            return job.waitForCompletion(true);
        }
}
