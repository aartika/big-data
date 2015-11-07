import com.google.common.base.Optional;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class SimilarityCalculatorReducer extends Reducer<ProductPair, RatingPair, ProductPair, DoubleWritable> {

    @Override
    protected void reduce(ProductPair key, Iterable<RatingPair> values, Context context) throws IOException, InterruptedException {
        Optional<Double> similarity = cosineSimilarity(values.iterator());
        if (similarity.isPresent()) {
            context.write(
                    new ProductPair(key.getProductId1(), key.getProductId2()),
                    new DoubleWritable(similarity.get())
            );
        }

    }

    private Optional<Double> cosineSimilarity(Iterator<RatingPair> ratingPairIterator) {
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
        }

        return countOfPairs < 5 ? Optional.<Double>absent() :
                Optional.of(dotProduct / (Math.sqrt(sumOfSquares1) * Math.sqrt(sumOfSquares2)));
    }
}
