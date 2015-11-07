import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class ProductSimilarityTuple implements WritableComparable<ProductSimilarityTuple> {

    private String productId;
    private double similarity;

    public String getProductId() {
        return productId;
    }

    public double getSimilarity() {
        return similarity;
    }

    public ProductSimilarityTuple() {}

    public ProductSimilarityTuple(String productId, double similarity) {
        this.productId = productId;
        this.similarity = similarity;
    }

    public int compareTo(@Nonnull ProductSimilarityTuple that) {
        return Double.compare(this.similarity, that.similarity);
    }

    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, productId);
        dataOutput.writeDouble(similarity);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.productId = Text.readString(dataInput);
        this.similarity = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "ProductSimilarityTuple{" +
                "productId='" + productId + '\'' +
                ", similarity=" + similarity +
                '}';
    }
}
