import org.apache.hadoop.io.*;
import java.io.*;

public class EdgeWritable implements Writable{
	   private int inEdge;
       private int outEdge;
       
       public EdgeWritable(int i, int o){
       		inEdge = i;
       		outEdge = o;
       }
       public EdgeWritable(){
       }
       
       public void write(DataOutput out) throws IOException {
         out.writeInt(inEdge);
         out.writeInt(outEdge);
       }
       
       public void readFields(DataInput in) throws IOException {
         inEdge = in.readInt();
         outEdge = in.readInt();
       }     
       
       public int getInEdge(){
       		return inEdge;
       }
       public int getOutEdge(){
       		return outEdge;
       }
       
       public String toString(){
       		return "("+inEdge+","+outEdge+")";
       }
	}
