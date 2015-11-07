import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Created by aartika.rai on 07/11/15.
 */
public class RatingPair implements Writable {

    private double rating1;
    private double rating2;

    public RatingPair() {}

    public double getRating1() {
        return rating1;
    }

    public double getRating2() {
        return rating2;
    }

    public RatingPair(double rating1, double rating2) {
        this.rating1 = rating1;
        this.rating2 = rating2;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(rating1);
        dataOutput.writeDouble(rating2);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.rating1 = dataInput.readDouble();
        this.rating2 = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RatingPair)) return false;
        RatingPair that = (RatingPair) o;
        return Objects.equals(rating1, that.rating1) &&
                Objects.equals(rating2, that.rating2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rating1, rating2);
    }
}
