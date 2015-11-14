import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Collections2.transform;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class ProductSimilarityTupleList implements Writable {

    private List<ProductSimilarityTuple> tuples;

    public ProductSimilarityTupleList() {}

    public ProductSimilarityTupleList(List<ProductSimilarityTuple> tuples) {
        this.tuples = ImmutableList.copyOf(tuples);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(tuples.size());
        for (ProductSimilarityTuple tuple : tuples) {
            tuple.write(dataOutput);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        this.tuples = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            ProductSimilarityTuple tuple = new ProductSimilarityTuple();
            tuple.readFields(dataInput);
            this.tuples.add(tuple);
        }
    }

    @Override
    public String toString() {
        return "{" + Joiner.on(", ").join(transform(tuples,
                new Function<ProductSimilarityTuple, String>() {
                    public String apply(ProductSimilarityTuple tuple) {
                        return tuple.getProductId() + " : " + tuple.getSimilarity();
                    }
                })) + "}";
    }
}
