import org.apache.giraph.GiraphRunner;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

public class Twitter2Facebook extends
  BasicComputation<Text, DoubleWritable, DoubleWritable, Text> {

  // the following two constants will represent existing edges and the ones we
  // created to model Facebook connections
  static final DoubleWritable ORIGINAL_EDGE = new DoubleWritable(1);
  static final DoubleWritable SYNTHETIC_EDGE = new DoubleWritable(2);

  @Override
  public void compute(Vertex<Text, DoubleWritable, DoubleWritable> vertex,
                      Iterable<Text> messages) {

    if (getSuperstep() == 0) {
      sendMessageToAllEdges(vertex, vertex.getId());  // this will broadcast this vertex’s ID to all its 
                                                                                     // neighbours 
    } else {
      for (Text m : messages) {
        DoubleWritable edgeValue = vertex.getEdgeValue(m);
        if (edgeValue == null) {
          // if the edge didn’t exist before we received a message create a new one and label it as such
          vertex.addEdge(EdgeFactory.create(m, SYNTHETIC_EDGE));
        } else {
          // for existing edges just label them as such
          vertex.setEdgeValue(m, ORIGINAL_EDGE);
        }
      }
    }

    vertex.voteToHalt();
  }
}

