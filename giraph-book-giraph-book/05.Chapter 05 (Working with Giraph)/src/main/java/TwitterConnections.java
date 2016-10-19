import org.apache.giraph.GiraphRunner;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

public class TwitterConnections extends
   BasicComputation<Text, DoubleWritable, DoubleWritable, Text> { // note that the last type variable is no
                                                                  // longer NullWritable. This is because
                                                                  // we are going to send Text messages to 
                                                                  // vertex’s neighbours as a way to notify
                                                                  // target vertices that we are connected
                                                                  // to them.  
  @Override
  public void compute(Vertex<Text, DoubleWritable, DoubleWritable> vertex,
                      Iterable<Text> messages) {

    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(vertex.getNumEdges()));
      sendMessageToAllEdges(vertex, new Text()); // the content of the message is an empty Text string,
                                                 // since we are not interested in the content, but rather  
                                                 // just the fact that a message was generated
    } else {                               // this indicates we’re now in super step > 0 
      int inDegree = 0;                                               
      for (Text m : messages) {                                  // here we simply count overall number of messages
        inDegree++;                                              // received by this vertex, and…
      }
      vertex.setValue(new DoubleWritable(vertex.getValue().get() +
                                         inDegree));   // … finally setting the overall number of messages to be this 
                                                       // vertex’s label. This is the value of an in-out-degree
    }

    vertex.voteToHalt();                                                 
  }  
}
