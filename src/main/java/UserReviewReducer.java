import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by rohit on 11/20/2015.
 */
public class UserReviewReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        long total = 0;
        double rating = 0;
        double helpful = 0;
        double length = 0;
        for( Text v : values){
            String s = v.toString();
            String[] t = s.split(","); //rating,length
            rating += Double.valueOf(t[0]);
            helpful += Double.valueOf(t[0]);
            length += Double.valueOf(t[2]);
            total += 1;
        }
        Double avgRating = rating / total;
        Double avgLength = length / total;
        Double avgHelpful = helpful / total;
        context.write(key, new Text(total + "," + avgRating + "," + avgHelpful + "," + avgLength));
    }
}
