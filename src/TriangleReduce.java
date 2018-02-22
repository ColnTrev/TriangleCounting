import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TriangleReduce extends Reducer<EdgePair,IntWritable,IntWritable,Triangle> {
    @Override
    protected void reduce(EdgePair node, Iterable<IntWritable> connectors, Context context) throws IOException,InterruptedException {
        for(IntWritable connector : connectors){
            if(connector != null){ // write only if there is an edge pair connected with a non-null connector
                context.write(new IntWritable(0),new Triangle(node.getStart(), node.getEnd(), connector.get()));
            }
        }
    }
}
