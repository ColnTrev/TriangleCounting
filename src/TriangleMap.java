import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TriangleMap extends Mapper<EdgePair,IntWritable,EdgePair,IntWritable>{
    private final Map<EdgePair,IntWritable> edgeMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException,InterruptedException{
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path edges = new Path(conf.get("graphEdge.path"));
        try(SequenceFile.Reader reader = new SequenceFile.Reader(conf,SequenceFile.Reader.file(edges))) {
            EdgePair edge = new EdgePair();
            IntWritable connector = new IntWritable();
            while(reader.next(edge, connector)) {
                edgeMap.put(edge, connector);
            }
        }
    }

    @Override
    public void map(IntWritable key, IntWritable value, Context context) throws IOException,InterruptedException {
        for(HashMap.Entry<EdgePair,IntWritable> entry : edgeMap.entrySet()){
            context.write(entry.getKey(), entry.getValue());
        }

    }
}
