import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by colntrev on 2/18/18.
 */
public class Triangle implements WritableComparable<Triangle>{
    private int[] nodes;
    public Triangle(){
        super();
        nodes = new int[3];
    }
    public Triangle(int a, int b, int c){
        super();
        nodes = new int[]{a,b,c};
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    public void sortNodes(){
        Arrays.sort(nodes);
    }
    @Override
    public int compareTo(Triangle triangle) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for(int i = 0; i < nodes.length; i++){
            dataOutput.writeInt(nodes[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        nodes = new int[3]; // all triangles have 3 nodes
        for(int i = 0; i < nodes.length; i++){
            nodes[i] = dataInput.readInt();
        }
    }
}
