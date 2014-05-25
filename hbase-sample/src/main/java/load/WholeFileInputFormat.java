package load;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

/**
 * The Class WholeFileInputFormat, 
 * taken with slight modifications from
 * {@link https://github.com/tomwhite/hadoop-book/blob/master/ch07/src/main/java/WholeFileInputFormat.java}
 *
 * @author Tom White
 */
public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, BytesWritable> {
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.lib.input.FileInputFormat#isSplitable(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public RecordReader<NullWritable, BytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    WholeFileRecordReader reader = new WholeFileRecordReader();
    reader.initialize(split, context);
    return reader;
  }
}
