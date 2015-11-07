import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class RecommenderReducer extends Reducer<Text, ProductSimilarityTuple, Text, ProductSimilarityTupleList> {

    @Override
    protected void reduce(Text key, Iterable<ProductSimilarityTuple> values, Context context)
            throws IOException, InterruptedException {

        context.write(
                new Text(key.toString()),
                new ProductSimilarityTupleList(mostSimilarProducts(values.iterator(), 5))
        );
    }

    private List<ProductSimilarityTuple> mostSimilarProducts(Iterator<ProductSimilarityTuple> tupleIterator, int n) {
        PriorityQueue<ProductSimilarityTuple> mostSimilar = new PriorityQueue<ProductSimilarityTuple>();
        while (tupleIterator.hasNext()) {
            ProductSimilarityTuple similarityTuple = tupleIterator.next();
            if (mostSimilar.size() < n) {
                mostSimilar.add(new ProductSimilarityTuple(similarityTuple.getProductId(), similarityTuple.getSimilarity()));
            } else if (mostSimilar.peek().compareTo(similarityTuple) < 0) {
                mostSimilar.poll();
                mostSimilar.add(new ProductSimilarityTuple(similarityTuple.getProductId(), similarityTuple.getSimilarity()));
            }
        }
        return Lists.newArrayList(mostSimilar);
    }
}
