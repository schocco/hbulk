package load;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * The Class WholeFileRecordReader.
 * Taken with slight modifications from
 * {@link https://github.com/tomwhite/hadoop-book/blob/master/ch07/src/main/java/WholeFileInputFormat.java}
 * 
 * @author Tom White
 */
class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

private FileSplit fileSplit;
private Configuration conf;
private BytesWritable value = new BytesWritable();
private boolean processed = false;

/* (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
 */
@Override
public void initialize(InputSplit split, TaskAttemptContext context)
   throws IOException, InterruptedException {
 this.fileSplit = (FileSplit) split;
 this.conf = context.getConfiguration();
}

/* (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
 */
@Override
public boolean nextKeyValue() throws IOException, InterruptedException {
 if (!processed) {
   byte[] contents = new byte[(int) fileSplit.getLength()];
   Path file = fileSplit.getPath();
   FileSystem fs = file.getFileSystem(conf);
   FSDataInputStream in = null;
   try {
     in = fs.open(file);
     IOUtils.readFully(in, contents, 0, contents.length);
     value.set(contents, 0, contents.length);
   } finally {
     IOUtils.closeStream(in);
   }
   processed = true;
   return true;
 }
 return false;
}

/* (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
 */
@Override
public NullWritable getCurrentKey() throws IOException, InterruptedException {
 return NullWritable.get();
}

/* (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
 */
@Override
public BytesWritable getCurrentValue() throws IOException,
   InterruptedException {
 return value;
}

/* (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
 */
@Override
public float getProgress() throws IOException {
 return processed ? 1.0f : 0.0f;
}

/* (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.RecordReader#close()
 */
@Override
public void close() throws IOException {
 // do nothing
}
}
