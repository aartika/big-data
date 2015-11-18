import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

/**
 * Created by rohit on 11/19/2015.
 */
// "product.productId","product.title","avg.score","count.score","product.avgPrice","avg.length"
public class UserReviewMapper  extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject json = new JSONObject(value.toString());
            double rating =  (Double) json.get("overall");
            String asin = (String) json.get("asin");
            String reviewerId = (String) json.get("reviewerID");
            int length = ((String) json.get("reviewText")).length();
            Text t = new Text("" + rating + "," + length);
            context.write(new Text(asin), t );
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
