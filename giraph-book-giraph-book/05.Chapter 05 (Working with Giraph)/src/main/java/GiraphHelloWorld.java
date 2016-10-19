import org.apache.giraph.edge.Edge;
import org.apache.giraph.GiraphRunner;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ToolRunner;


public class GiraphHelloWorld extends            // Giraph applications are custom classes that
   BasicComputation<IntWritable, IntWritable,    // typically use BasicComputation class for all its defaults…
                    NullWritable, NullWritable> {
  @Override
  public void compute(Vertex<IntWritable,        // …except for the compute method that has to be defined
                      IntWritable, NullWritable> vertex,
                  Iterable<NullWritable> messages) {
    System.out.print("Hello world from the: " +
      vertex.getId().toString() + " who is following:");  // priting a “Hello World” message for the current vertex
 
    for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {  // iterating over vertex’s neighbors
      System.out.print(" " + e.getTargetVertexId());
    }  
    System.out.println("");
 
    vertex.voteToHalt();  // signaling the end of the current BSP computation for the current vertex 
  }  
 
  public static void main(String[] args) throws Exception {   // main() method is not strictly speaking required,
    System.exit(ToolRunner.run(new GiraphRunner(), args));    // but it could be useful when using Hadoop’s
  }                                                           // executor script instead of Giraph’s
}

