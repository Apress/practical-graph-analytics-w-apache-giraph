import org.apache.giraph.edge.Edge;          
import org.apache.giraph.GiraphRunner;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ToolRunner;

public class GiraphHelloWorld2 extends                                      
  BasicComputation<Text, DoubleWritable, DoubleWritable, NullWritable> { // note a change in type 
                                                                         // variables to match new input
  @Override
  public void compute(Vertex<Text,DoubleWritable,DoubleWritable> vertex, // a matching change in type
                      Iterable<NullWritable> messages) {                 // variables
    System.out.print("Hello world from the: " +
      vertex.getId().toString() + " who is following:");                 
 
    for (Edge<Text, DoubleWritable> e : vertex.getEdges()) {             // one more change to types
      System.out.print(" " + e.getTargetVertexId());
    }  
    System.out.println("");
 
    vertex.voteToHalt();                                                 
  }  
}
