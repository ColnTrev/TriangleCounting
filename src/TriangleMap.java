

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TriangleMap extends Mapper<EdgePair,IntWritable,EdgePair,IntWritable>{
    private final Map<EdgePair,ArrayList<IntWritable>> edgeMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException,InterruptedException{
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path edges = new Path(conf.get("graphEdge.path"));
        try(SequenceFile.Reader reader = new SequenceFile.Reader(conf,SequenceFile.Reader.file(edges))) {
            EdgePair edge = new EdgePair();
            IntWritable connector = new IntWritable();
            while(reader.next(edge, connector)) {
                if(!edgeMap.containsKey(edge)) {
                    edgeMap.put(edge, new ArrayList<>());
                }
                edgeMap.get(edge).add(connector);
            }
        }
    }

    @Override
    public void map(EdgePair pair, IntWritable connector, Context context) throws IOException,InterruptedException {
        for(HashMap.Entry<EdgePair,ArrayList<IntWritable>> entry : edgeMap.entrySet()){
            for(IntWritable node : entry.getValue()) {
                context.write(entry.getKey(), node);
            }
        }
    }
}
