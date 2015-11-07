import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Created by rohit on 11/6/2015.
 */
public class UserRating implements Writable {

    private String productId;
    private double rating;

    public UserRating(){}

    public String getProductId() {
        return productId;
    }

    public double getRating() {
        return rating;
    }

    public UserRating(String userId, double rating){
        this.productId = userId;
        this.rating = rating;
    }

    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, productId);
        dataOutput.writeDouble(rating);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.productId = Text.readString(dataInput);
        this.rating = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserRating)) return false;
        UserRating that = (UserRating) o;
        return Objects.equals(rating, that.rating) &&
                Objects.equals(productId, that.productId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId, rating);
    }
}
