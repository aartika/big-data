import com.google.gson.JsonArray;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rohit on 11/20/2015.
 */
public class UserReviewMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject json = new JSONObject(value.toString());
            double rating =  (Double) json.get("overall");
            String reviewerId = (String) json.get("reviewerID");
            int length = ((String) json.get("reviewText")).length();
            JSONArray helpful = (JSONArray) json.get("helpful");
            int h = helpful.getInt(0);
            Text t = new Text("" + rating + "," + h + "," + length);
            context.write(new Text(reviewerId), t );
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
