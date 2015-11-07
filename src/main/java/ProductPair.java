import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class ProductPair implements WritableComparable<ProductPair> {

    private String productId1;
    private String productId2;

    public ProductPair() {}

    public String getProductId1() {
        return productId1;
    }

    public String getProductId2() {
        return productId2;
    }

    public ProductPair(String productId1, String productId2) {
        this.productId1 = productId1;
        this.productId2 = productId2;
    }

    public int compareTo(@Nonnull ProductPair that) {
        int ret;
        if ((ret = this.productId1.compareTo(that.productId1)) != 0) {
            return ret;
        } else {
            return this.productId2.compareTo(that.productId2);
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, productId1);
        Text.writeString(dataOutput, productId2);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.productId1 = Text.readString(dataInput);
        this.productId2 = Text.readString(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProductPair)) return false;
        ProductPair that = (ProductPair) o;
        return Objects.equals(productId1, that.productId1) &&
                Objects.equals(productId2, that.productId2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId1, productId2);
    }

    @Override
    public String toString() {
        return "ProductPair{" +
                "productId1='" + productId1 + '\'' +
                ", productId2='" + productId2 + '\'' +
                '}';
    }
}
