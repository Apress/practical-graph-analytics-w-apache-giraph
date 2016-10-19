import org.apache.giraph.GiraphRunner;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import java.io.IOException;


public class GenerateTwitter extends
  BasicComputation<Text, DoubleWritable, DoubleWritable, Text> {

  // two static arrays defining the same graph as input data from Listing 5.10
  private static final String[] twitterMembers = { "seed", // seed is a vertex that is only required for bootstrap
    "John", "Peter", "Mark", "Anne", "Natalie", "Jack", "Julia" };
  private static final byte[][] twitterFollowership = {{0},
    {2},   {},      {1, 4},  {2, 7}, {1, 2, 4}, {3, 4}, {3, 5}};

  @Override
  public void compute(Vertex<Text, DoubleWritable, DoubleWritable> vertex,
                      Iterable<Text> messages) throws IOException {  // we have accept the fact that mutating 
                                                                                     // graph topology can lead to IOException thrown 

    if (getSuperstep() == 0) {
      // in the very first superstep we’re iterating over the data recorded in two static arrays and
      // create all of our graph structure
      for (int i = 1; i < twitterFollowership.length; i++) {
        Text destVertexID = new Text(twitterMembers[i]);
        addVertexRequest(destVertexID, new DoubleWritable(0));
      
        for (byte neighbour : twitterFollowership[i]) {
          addEdgeRequest(destVertexID, EdgeFactory.create(
                  new Text(twitterMembers[neighbour]), 
                  new DoubleWritable(0)));
        }
      }
   
      removeVertexRequest(new Text("seed"));   // since the initial vertex wasn’t really part of our graph and
                                                                            // was only required to get us started we can remove it now
    } else {
      vertex.voteToHalt();  // note that we are halting right after superstep 0, but not at the superstep 0. 
                                        // This is to let our new graph topology be reflected in the output.
    }
  }
}


