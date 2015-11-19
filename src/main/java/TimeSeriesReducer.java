import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by rohit on 11/19/2015.
 */
public class TimeSeriesReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
        int total = 0;
        for( IntWritable v : values){
            total += v.get();
        }
        context.write(key, new Text(Integer.toString(total)));
    }
}
