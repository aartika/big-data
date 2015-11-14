import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by aartika.rai on 06/11/15.
 */
public class MovieRecommender extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MovieRecommender(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if (args.length != 4) {
            System.err.println("Arguments : reviewsPath ratingPairPath similarityPath finalOutputPath");
            return 1;
        }
        conf.set("reviewsPath", args[0]);
        conf.set("ratingPairPath", args[1]);
        conf.set("similarityPath", args[2]);
        conf.set("finalOutputPath", args[3]);

        return runPairMakerJob(conf) && runSimilarityCalculatorJob(conf) && runRecommenderJob(conf) ? 0 : 1;
    }

    private static boolean runPairMakerJob(Configuration conf) throws Exception {

        Job job = Job.getInstance(conf, "Pair Maker");

        job.addFileToClassPath(new Path("/user/aartika.rai/big-data-project-1.0-SNAPSHOT-jar-with-dependencies.jar"));
        Path outputDir = new Path(conf.get("ratingPairPath"));
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, conf.get("reviewsPath"));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputDir);
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, ProductPair.class, RatingPair.class);

        job.setMapperClass(PairMakerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserRating.class);
        job.setReducerClass(PairMakerReducer.class);

        return job.waitForCompletion(true);
    }

    private static boolean runSimilarityCalculatorJob(Configuration conf) throws Exception {

        Job job = Job.getInstance(conf, "Similarity Calculator");

        job.addFileToClassPath(new Path("/user/aartika.rai/big-data-project-1.0-SNAPSHOT-jar-with-dependencies.jar"));
        Path outputDir = new Path(conf.get("similarityPath"));
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.<ProductPair, RatingPair>setInputPaths(job, conf.get("ratingPairPath") + "/seq");
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputDir);
        MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, ProductPair.class, DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, NullWritable.class, Text.class);

        job.setMapperClass(SimilarityCalculatorMapper.class);
        job.setMapOutputKeyClass(ProductPair.class);
        job.setMapOutputValueClass(RatingPair.class);
        job.setReducerClass(SimilarityCalculatorReducer.class);

        return job.waitForCompletion(true);
    }

    private static boolean runRecommenderJob(Configuration conf) throws Exception {

        Job job = Job.getInstance(conf, "Recommender");

        job.addFileToClassPath(new Path("/user/aartika.rai/big-data-project-1.0-SNAPSHOT-jar-with-dependencies.jar"));
        Path outputDir = new Path(conf.get("finalOutputPath"));
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ProductSimilarityTupleList.class);
        SequenceFileInputFormat.<ProductPair, DoubleWritable>setInputPaths(job, conf.get("similarityPath") + "/seq");
        TextOutputFormat.<Text, ProductSimilarityTupleList>setOutputPath(job, outputDir);

        job.setMapperClass(RecommenderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ProductSimilarityTuple.class);
        job.setReducerClass(RecommenderReducer.class);

        return job.waitForCompletion(true);
    }
}
