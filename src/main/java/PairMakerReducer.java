import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.List;

/**
 * Created by aartika.rai on 06/11/15.
 */
public class PairMakerReducer extends Reducer<Text, UserRating, ProductPair, RatingPair> {

    MultipleOutputs multipleOutputs;
    String outputPath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.multipleOutputs = new MultipleOutputs(context);
        this.outputPath = context.getConfiguration().get("ratingPairPath");
    }

    @Override
    protected void reduce(Text key, Iterable<UserRating> values, Context context) throws IOException, InterruptedException {

        final List<UserRating> ratings = Lists.newArrayList(Iterables.transform(values, new Function<UserRating, UserRating>() {
            public UserRating apply(UserRating userRating) {
                return new UserRating(userRating.getProductId(), userRating.getRating());
            }
        }));

        for (UserRating rating : ratings) {
            multipleOutputs.write("text", NullWritable.get(),
                    new Text(Joiner.on("\t").join(key.toString(), rating.getProductId(), rating.getRating())),
                    this.outputPath + "/" + "text/part_" + context.getTaskAttemptID()
            );
            context.progress();
        }

        for (int i = 0; i < ratings.size(); i++) {
            for (int j = i + 1; j < ratings.size(); j++) {
                 if (ratings.get(i).getProductId().compareTo(ratings.get(j).getProductId()) < 0) {
                     writePair(ratings.get(i), ratings.get(j), context);
                 } else {
                     writePair(ratings.get(j), ratings.get(i), context);
                 }
                context.progress();
            }
        }
    }

    private void writePair(UserRating rating1, UserRating rating2, Context context) throws IOException, InterruptedException {
        multipleOutputs.write("seq", new ProductPair(rating1.getProductId(), rating2.getProductId()),
                new RatingPair(rating1.getRating(), rating2.getRating()),
                this.outputPath +  "/" + "seq/part_" + context.getTaskAttemptID()
        );
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.multipleOutputs.close();
        super.cleanup(context);
    }
}
