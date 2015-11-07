import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

/**
 * Created by aartika.rai on 06/11/15.
 */
public class PairMakerMapper extends Mapper<LongWritable, Text, Text, UserRating> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject json = new JSONObject(value.toString());
            String asin = (String) json.get("asin");
            String reviewerId = (String) json.get("reviewerID");
            double rating =  (Double) json.get("overall");
            UserRating userRating = new UserRating(asin, rating);
            context.write(new Text(reviewerId), userRating);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
