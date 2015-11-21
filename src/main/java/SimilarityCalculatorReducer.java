import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class SimilarityCalculatorReducer extends Reducer<ProductPair, RatingPair, ProductPair, DoubleWritable> {

    private int minPairCount;
    private MultipleOutputs multipleOutputs;
    private String outputPath;
    private CorrelationType correlationStrategy;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.minPairCount = conf.getInt("minPairCount", 5);
        this.multipleOutputs = new MultipleOutputs(context);
        this.outputPath = conf.get("similarityPath");
        this.correlationStrategy = CorrelationType.valueOf(conf.get("correlationMethod", "COSINE"));
    }

    @Override
    protected void reduce(ProductPair key, Iterable<RatingPair> values, Context context) throws IOException, InterruptedException {
        Optional<Double> similarity = this.correlationStrategy.calculateSimilarity(values.iterator(), this.minPairCount, context);
        if (similarity.isPresent()) {
            multipleOutputs.write("seq",
                    new ProductPair(key.getProductId1(), key.getProductId2()),
                    new DoubleWritable(similarity.get()),
                    outputPath + "/" + "seq/part_" + context.getTaskAttemptID()
            );
            multipleOutputs.write("text", NullWritable.get(),
                    new Text(Joiner.on("\t").join(key.getProductId1(), key.getProductId2(), similarity.get())),
                    outputPath + "/" + "text/part_" + context.getTaskAttemptID()
            );
            multipleOutputs.write("text", NullWritable.get(),
                    new Text(Joiner.on("\t").join(key.getProductId2(), key.getProductId1(), similarity.get())),
                    outputPath + "/" + "text/part_" + context.getTaskAttemptID()
            );
        }
    }

    enum CorrelationType {

        PEARSON {
            @Override
            public Optional<Double> calculateSimilarity(Iterator<RatingPair> ratingPairIterator, int minPairCount, Context context) {
                int n = 0;
                double sumxy = 0;
                double sumx = 0;
                double sumy = 0;
                double sumx2 = 0;
                double sumy2 = 0;
                while( ratingPairIterator.hasNext()) {
                    n++;
                    RatingPair pair = ratingPairIterator.next();
                    sumx += pair.getRating1();
                    sumy += pair.getRating2();
                    sumxy += pair.getRating1() * pair.getRating2();
                    sumx2 += pair.getRating1() * pair.getRating1();
                    sumy2 += pair.getRating2() * pair.getRating2();
                    context.progress();
                }
                double corr = (n *sumxy - sumx*sumy)/ (
                        Math.sqrt( n*sumx2 - sumx*sumx) * Math.sqrt( n*sumy2 - sumy*sumy )
                );
                return n < minPairCount ? Optional.<Double>absent() : Optional.of(corr);
            }
        },

        COSINE {
            @Override
            public Optional<Double> calculateSimilarity(Iterator<RatingPair> ratingPairIterator, int minPairCount, Context context) {
                double dotProduct = 0.0;
                double sumOfSquares1 = 0.0;
                double sumOfSquares2 = 0.0;
                int countOfPairs = 0;

                while (ratingPairIterator.hasNext()) {
                    countOfPairs++;
                    RatingPair pair = ratingPairIterator.next();
                    dotProduct += pair.getRating1() * pair.getRating2();
                    sumOfSquares1 += pair.getRating1() * pair.getRating1();
                    sumOfSquares2 += pair.getRating2() * pair.getRating2();
                    context.progress();
                }

                return countOfPairs < minPairCount ? Optional.<Double>absent() :
                        Optional.of(dotProduct / (Math.sqrt(sumOfSquares1) * Math.sqrt(sumOfSquares2)));
            }

        },

        INTERSECTION {
            @Override
            public Optional<Double> calculateSimilarity(Iterator<RatingPair> ratingPairIterator, int minPairCount, Context context) {
                int count = 0;
                while (ratingPairIterator.hasNext()) {
                    count++;
                }
                return Optional.of((double) count);
            }
        };

        public abstract Optional<Double> calculateSimilarity(Iterator<RatingPair> ratingPairIterator, int minPairCount, Context context);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.multipleOutputs.close();
        super.cleanup(context);
    }
}
