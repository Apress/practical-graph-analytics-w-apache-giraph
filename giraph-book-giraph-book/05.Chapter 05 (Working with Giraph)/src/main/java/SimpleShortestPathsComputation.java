import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import java.io.IOException;

public class SimpleShortestPathsComputation extends 
  BasicComputation<LongWritable, DoubleWritable, 
                      FloatWritable, DoubleWritable> {

  // This is how we will be passing an ID of a starting vertex: via command line argument given
  // to the giraph execution utility.
  public static final LongConfOption SOURCE_ID =
    new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
                       "The shortest paths id");

  private static final Logger LOG =
    Logger.getLogger(SimpleShortestPathsComputation.class);
     
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }
     
  @Override
  public void compute(
    Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));           
    }
    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;          
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }
    if (LOG.isDebugEnabled()) { // this is how real-world Giraph applications handle debug output
      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + 
              minDist +	" vertex value = " + vertex.getValue());
      }
    // the following is the guts of the shortest path algorithm
    if (minDist < vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        double distance = minDist + edge.getValue().get();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
                     edge.getTargetVertexId() + " = " + distance);
          }
          sendMessage(edge.getTargetVertexId(), 
                      new DoubleWritable(distance));
        }
      }
      vertex.voteToHalt();
   }
}
