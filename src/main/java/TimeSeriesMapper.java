import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by rohit on 11/19/2015.
 */
// "product.productId","product.title","avg.score","count.score","product.avgPrice","avg.length"
public class TimeSeriesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject json = new JSONObject(value.toString());
            Integer unixtime =  (Integer) json.get("unixReviewTime");
            Date date = new Date(unixtime*1000L); // *1000 is to convert seconds to milliseconds
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM"); // the format of your date
            String yearMonth = sdf.format(date);
            System.out.println(yearMonth);
            context.write(new Text(yearMonth), new IntWritable(1) );
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
