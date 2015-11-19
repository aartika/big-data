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
public class UserReviewOutput extends Configured implements Tool{

        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new Configuration(), new UserReviewOutput(), args);
            System.exit(res);
        }

        public int run(String[] args) throws Exception {
            Configuration conf = getConf();
            if (args.length != 2) {
                System.err.println("Arguments : reviewsPath  userreviewseries");
                return 1;
            }
            conf.set("reviewsPath", args[0]);
            conf.set("userreviewseries", args[1]);
            //conf.set("mapreduce.job.user.classpath.first", "true");

            return  frequencyCountjob(conf) ? 0 : 1;
        }



        private static boolean frequencyCountjob(Configuration conf) throws Exception {

            Job job = Job.getInstance(conf, "Time Series");

            job.addFileToClassPath(new Path("/code/big-data-project-1.0-SNAPSHOT-jar-with-dependencies.jar"));
            Path outputDir = new Path(conf.get("userreviewseries"));
            FileSystem hdfs = FileSystem.get(conf);
            if (hdfs.exists(outputDir)) {
                hdfs.delete(outputDir, true);
            }
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job, conf.get("reviewsPath"));
            TextOutputFormat.<Text, Text>setOutputPath(job, outputDir);

            job.setMapperClass(UserReviewMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(UserReviewReducer.class);

            return job.waitForCompletion(true);
        }
}
