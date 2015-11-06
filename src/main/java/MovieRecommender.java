import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * Created by aartika.rai on 06/11/15.
 */
public class MovieRecommender extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        ToolRunner.run(conf, new MovieRecommender(), args);
    }

    public int run(String[] strings) throws Exception {
        //set input output arguments in conf
        Configuration conf = getConf();
        return runRecommenderJob(conf);
    }

    private int runRecommenderJob(Configuration conf) throws Exception {

        Job job = Job.getInstance(conf);
        TextInputFormat.setInputPaths(job, conf.get("inputPath"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
