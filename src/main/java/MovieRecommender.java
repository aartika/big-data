import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Created by aartika.rai on 06/11/15.
 */
public class MovieRecommender {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Arguments : reviewsPath ratingPairPath similarityPath finalOutputPath");
            return;
        }
        Configuration conf = new Configuration();
        conf.set("reviewsPath", args[0]);
        conf.set("ratingPairPath", args[1]);
        conf.set("similarityPath", args[2]);
        conf.set("finalOutputPath", args[3]);

        System.exit(runPairMakerJob(conf) && runSimilarityCalculatorJob(conf) && runRecommenderJob(conf) ? 0 : 1);
    }

    private static boolean runPairMakerJob(Configuration conf) throws Exception {

        Job job = Job.getInstance(conf, "Pair Maker");

        job.addFileToClassPath(new Path("/user/aartika.rai/big-data-project-1.0-SNAPSHOT-jar-with-dependencies.jar"));
        Path outputDir = new Path(conf.get("ratingPairPath"));
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(ProductPair.class);
        job.setOutputValueClass(RatingPair.class);
        TextInputFormat.setInputPaths(job, conf.get("reviewsPath"));
        SequenceFileOutputFormat.<ProductPair, RatingPair>setOutputPath(job, outputDir);

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
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(ProductPair.class);
        job.setOutputValueClass(DoubleWritable.class);
        SequenceFileInputFormat.<ProductPair, RatingPair>setInputPaths(job, conf.get("ratingPairPath"));
        SequenceFileOutputFormat.<ProductPair, DoubleWritable>setOutputPath(job, outputDir);

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
        SequenceFileInputFormat.<ProductPair, DoubleWritable>setInputPaths(job, conf.get("similarityPath"));
        TextOutputFormat.<Text, ProductSimilarityTupleList>setOutputPath(job, outputDir);

        job.setMapperClass(RecommenderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ProductSimilarityTuple.class);
        job.setReducerClass(RecommenderReducer.class);

        return job.waitForCompletion(true);
    }
}
