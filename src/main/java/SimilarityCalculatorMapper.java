import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class SimilarityCalculatorMapper extends Mapper<ProductPair, RatingPair, ProductPair, RatingPair> {

    @Override
    protected void map(ProductPair key, RatingPair value, Context context) throws IOException, InterruptedException {
        context.write(
                new ProductPair(key.getProductId1(), key.getProductId2()),
                new RatingPair(value.getRating1(), value.getRating2())
        );
    }
}
