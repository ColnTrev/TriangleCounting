import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by colntrev on 2/18/18.
 */
public class GraphEdgeReduce extends Reducer<IntWritable,IntWritable,EdgePair,IntWritable> {
    @Override
    protected void reduce(IntWritable node, Iterable<IntWritable> edges, Context context) throws IOException,InterruptedException {

        List<IntWritable> cache = new ArrayList<>(); // add values from iterable here since iterable passes once
        //write edges in K,V pairs with K
        // example [2,1],(-)
        for(IntWritable edge : edges){
            cache.add(edge);
            EdgePair e = new EdgePair(node.get(), edge.get());
            context.write(e,null);
        }

        //write edges with V,V pairs with their K connection
        // example [1,3], [2]
        for(int i = 0; i < cache.size(); i++){
            for(int j = 0; j < cache.size(); j++){
                if(cache.get(i).get() != cache.get(j).get()){
                    EdgePair e = new EdgePair(cache.get(i).get(),cache.get(j).get());
                    context.write(e, node);
                }
            }
        }
    }
}
