import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


/**
 * Created by colntrev on 2/18/18.
 */
public class UniqueTriangleMap extends Mapper<IntWritable,Triangle,Triangle,IntWritable>{
    private final List<Triangle> uniques = new ArrayList<>();
    @Override
    protected void setup(Context context) throws IOException,InterruptedException{
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path triangles = new Path(conf.get("triangles.path"));
        try(SequenceFile.Reader reader = new SequenceFile.Reader(conf,SequenceFile.Reader.file(triangles))) {
            Triangle t = new Triangle();
            IntWritable key = new IntWritable();
            while(reader.next(key, t)){
                uniques.add(t);
            }
        }
    }

    @Override
    public void map(IntWritable key, Triangle triangle, Context context) throws IOException,InterruptedException {
        HashSet<Triangle> seen = new HashSet<>();
        for(Triangle t : uniques){
            t.sortNodes();
            if(!seen.contains(t)){
                seen.add(t);
                context.write(t, null);
            }
        }
    }
}
