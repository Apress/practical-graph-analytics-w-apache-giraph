import org.apache.giraph.combiner.MessageCombiner;
import org.apache.hadoop.io.IntWritable;

public class BitArrayCombiner
  extends MessageCombiner<IntWritable, IntWritable> {
  
   // the guts of each combiner is a method that defines how to
   // combine a new message into an overall message accumulator
  @Override
  public void combine(IntWritable vertexIndex,
                      IntWritable originalMessage, 
                      IntWritable messageToCombine) { 
    
      originalMessage.set((1<<messageToCombine.get()) | 
                          originalMessage.get());
  }
  
  // this a method that defines how to create an initial message accumulator  
  // in our case this simply creates a bit-map array with all bits set to 0 (no messages seen)  
  @Override
  public IntWritable createInitialMessage() {
    return new IntWritable(0);
  }
}
