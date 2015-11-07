import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class RecommenderMapper extends Mapper<ProductPair, DoubleWritable, Text, ProductSimilarityTuple> {

    @Override
    protected void map(ProductPair key, DoubleWritable value, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.getProductId1()), new ProductSimilarityTuple(key.getProductId2(), value.get()));
        context.write(new Text(key.getProductId2()), new ProductSimilarityTuple(key.getProductId1(), value.get()));
    }
}
