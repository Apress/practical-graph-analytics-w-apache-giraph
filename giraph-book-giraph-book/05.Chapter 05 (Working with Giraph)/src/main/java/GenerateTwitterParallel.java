import org.apache.giraph.GiraphRunner;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class GenerateTwitterParallel extends
  BasicComputation<Text, DoubleWritable, DoubleWritable, IntWritable> { // note how the type of the 
            // messages we send changed to IntWritable since we plan to pass integer IDs similar to what
            // we did when graph was represented as shown in Listing 5.1

  private static final String[] twitterMembers = { "",
            "John", "Peter", "Mark", "Anne", "Natalie", "Jack", "Julia" };
  private static final byte[][] twitterFollowership = {{0},
            {2},   {},      {1, 4},  {2, 7}, {1, 2, 4}, {3, 4}, {3, 5}};

  @Override
  public void compute(Vertex<Text, DoubleWritable, DoubleWritable> vertex,
                      Iterable<IntWritable> messages) throws IOException {

    if (getSuperstep() == 0) {
      for (int i = 1; i < twitterFollowership.length; i++) {
        Text destVertexID = new Text(twitterMembers[i]);
        for (byte neighbour : twitterFollowership[i]) {
          sendMessage(destVertexID, new IntWritable(neighbour));  // sending a message to a non-existent
                     // vertex is going to create that vertex and deliver a message to it. This is a small trick
                     // that allows us to not create vertices ahead of sending messages to them.
        }
      }

      removeVertexRequest(new Text("seed"));
    } else {
      // the nice thing about the following loop is that it is going to be executed for each vertex
      // created in the previous superstep. Thus edge creation will be done in parallel leveraging
      // combined power of all workers in the cluster, instead of being executed by a single worker
      // as in our previous example
      for (IntWritable m : messages) {                                 
        Text neighbour = new Text(twitterMembers[m.get()]);
        vertex.addEdge(EdgeFactory.create(neighbour,
                                          new DoubleWritable(0)));
      }

      vertex.voteToHalt();
    }
  }
}

