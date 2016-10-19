import org.junit.Test;
import org.junit.Assert;
import org.junit.Ignore;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.TextDoubleDoubleAdjacencyListVertexInputFormat;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;

public class TestGiraphApp  {
  final static String[] graphSeed = new String[] { "seed\t0" };
  final static int EXPECTED_VERTICES = 7; // The number of vertices we expect to
                                          // be created in the resulting graph

  @Ignore("The code this test validates needs to be fixed")
  @Test
  public void testNumberOfVertices() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration(); // Giraph configuration
       // object: it will configure our test Giraph run just like a combination of 
       // static configuration files and command line options we used in all of our 
       // previous examples
    conf.setComputationClass(GenerateTwitterParallel.class);
    conf.setVertexInputFormatClass(
      TextDoubleDoubleAdjacencyListVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
      AdjacencyListTextVertexOutputFormat.class);

    // The following is all that is required for our test to simulate
    // a full Giraph application run: the conf is how we configure the
    // simulation and graphSeed is an array of string simulating input
    // graph data. The result it returns back allows us to iterate over
    // the simulated output in order to check assumptions about what
    // we expect after the GenerateTwitterParallel code is done executing.
    Iterable<String> results =
          InternalVertexRunner.run(conf, graphSeed);

    // Iterating over the simulated output is how you check the
    // resulting graph structure. In our case we're simply counting
    // the number of lines in the simulated output.
    int totalVertices = 0;
    for (String s: results ) {
      totalVertices++;
    }
    // We expect a certain number of lines (vertex descriptions)
    // to be presented in the simulated output.
    Assert.assertEquals(EXPECTED_VERTICES, totalVertices);
  }
}
