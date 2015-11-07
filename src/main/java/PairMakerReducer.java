import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by aartika.rai on 06/11/15.
 */
public class PairMakerReducer extends Reducer<Text, UserRating, ProductPair, RatingPair> {

    @Override
    protected void reduce(Text key, Iterable<UserRating> values, Context context) throws IOException, InterruptedException {

        final List<UserRating> ratings = Lists.newArrayList(Iterables.transform(values, new Function<UserRating, UserRating>() {
            public UserRating apply(UserRating userRating) {
                return new UserRating(userRating.getProductId(), userRating.getRating());
            }
        }));

        for (int i = 0; i < ratings.size(); i++) {
            for (int j = i + 1; j < ratings.size(); j++) {
                 if (ratings.get(i).getProductId().compareTo(ratings.get(j).getProductId()) < 0) {
                     writePair(ratings.get(i), ratings.get(j), context);
                 } else {
                     writePair(ratings.get(j), ratings.get(i), context);
                 }
            }
        }
    }

    private void writePair(UserRating rating1, UserRating rating2, Context context) throws IOException, InterruptedException {
        context.write(
                new ProductPair(rating1.getProductId(), rating2.getProductId()),
                new RatingPair(rating1.getRating(), rating2.getRating())
        );
    }

}
