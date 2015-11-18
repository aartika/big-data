import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by rohit on 11/19/2015.
 */
public class UserReviewReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
        long total = 0;
        long rating = 0;
        long length = 0;
        for( Text v : values){
            String s = v.toString();
            String[] t = s.split(","); //rating,length
            rating += Long.valueOf(t[0]);
            length += Long.valueOf(t[1]);
        }
        Double avgRating = (double) rating / total;
        Double avgLength = (double) length / total;
        context.write(key, new Text(total + "," + avgRating + "" + avgLength));
    }
}
