/**
 * Created by colntrev on 2/18/18.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GraphEdgeMap extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable>{
    private final Map<IntWritable,IntWritable> edgeMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException,InterruptedException{
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path edges = new Path(conf.get("graphEdge.path"));
        try(SequenceFile.Reader reader = new SequenceFile.Reader(conf,SequenceFile.Reader.file(edges))) {
            IntWritable startNode = new IntWritable();
            IntWritable endNode = new IntWritable();
            while(reader.next(startNode, endNode)) {
                edgeMap.put(startNode, endNode);
            }
        }
    }

    @Override
    public void map(IntWritable key, IntWritable value, Context context) throws IOException,InterruptedException {
        for(HashMap.Entry<IntWritable,IntWritable> entry : edgeMap.entrySet()){
            context.write(entry.getKey(), entry.getValue());
            context.write(entry.getValue(), entry.getKey());
        }

    }
}
