import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by colntrev on 2/18/18.
 */
public class GraphEdgeReduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    @Override
    protected void reduce(IntWritable node, Iterable<IntWritable> edges, Context context) throws IOException,InterruptedException {
        //write edges in K,V pairs with K
        // example [2,1],(-)

        //write edges with V,V pairs with their K connection
        // example [1,3], [2]
    }
}
