package load;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.james.mime4j.MimeException;

public class BulkLoad {
	
	/** The name of the column family for the message header fields. */
	private static final String CF_META = "meta";
	
	/** The name of the column family for the message body. */
	private static final String CF_BODY = "body";

	/**
	 * The mapper class for the mapreduce job.
	 */
	public static class BulkLoadMap extends
			Mapper<NullWritable, BytesWritable, ImmutableBytesWritable, Put> {


		/** Column name for the message body. */
		private static final String COL_BODY = "body";
		
		/** Column name for message body meta data. */
		private static final String COL_BODY_META = "meta";

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		/**
		 * Creates an email object from the given value and creates a
		 * Put instance for each parsable email.
		 *
		 * @param key the key, not used
		 * @param value the email message
		 * @param context the context
		 * @throws IOException Signals that an I/O exception has occurred.
		 * @throws InterruptedException the interrupted exception
		 */
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			try {
				// parse email
				final Email email = new Email(value.getBytes());

				if(email.getId() != null){
					final byte[] rowKey = Bytes.toBytes(email.getId());
					// create put
					final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
					Put HPut = new Put(rowKey);
					// meta CF, each header field is one column
					for (String col : email.getHeaders().keySet()) {
						byte[] cell = Bytes.toBytes(email.getHeaders().get(col));
						HPut.add(Bytes.toBytes(CF_META), Bytes.toBytes(col), cell);
					}
					// body CF
					if(email.getBody() != null &! email.getBody().isEmpty()){
						HPut.add(Bytes.toBytes(CF_BODY), Bytes.toBytes(COL_BODY),
								Bytes.toBytes(email.getBody()));
					}
					if(email.getBodyMeta() != null &! email.getBodyMeta().isEmpty()){
						HPut.add(Bytes.toBytes(CF_BODY), Bytes.toBytes(COL_BODY_META),
								Bytes.toBytes(email.getBodyMeta()));
					}
					context.write(HKey, HPut);
				}
			} catch (MimeException e) {
				// could increment a failure counter here
			}

		}
	}

	/**
	 * Reads in all files of a given path recursively and passes them into
	 * the mapreduce job for processing.<br>
	 * Loads the resulting hfile in the hbase table if the job succeeded.
	 *
	 * @param args an array with 3 fields: 1. input path, 2. output path of the hfile, 3. table name to be used for the bulk load
	 * @throws ClassNotFoundException when job can't find the class
	 * @throws InterruptedException when job execution is interrupted
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws ClassNotFoundException, InterruptedException, Exception {

		Configuration conf = HBaseConfiguration.create();
		
		// create table for import
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(!admin.isTableAvailable(args[2].getBytes())){
			HTableDescriptor desc = new HTableDescriptor(args[2]);
			HColumnDescriptor meta = new HColumnDescriptor(CF_META.getBytes());
			HColumnDescriptor body = new HColumnDescriptor(CF_BODY.getBytes());
			desc.addFamily(meta);
			desc.addFamily(body);
			admin.createTable(desc);
		}
		admin.close();

		// read args
		// get the path string for all "all_documents" subfolders
		String inputPath = MailPaths.get(args[0]);
		String outputPath = args[1];
		HTable hTable = null;
		Job job;

		try {
			// configure job
			job = Job.getInstance(conf, "ExampleRead");
			job.setJarByClass(BulkLoad.class);
			job.setMapperClass(BulkLoad.BulkLoadMap.class);
			// mapper
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
			// speculation
			job.setSpeculativeExecution(false);
			job.setReduceSpeculativeExecution(false);
			// in/out format
			job.setInputFormatClass(WholeFileInputFormat.class);
			job.setOutputFormatClass(HFileOutputFormat2.class);
			
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

			hTable = new HTable(conf, args[2]);
			HFileOutputFormat2.configureIncrementalLoad(job, hTable);

			if (job.waitForCompletion(true)) {
				// Load generated HFiles into table
				LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
				//loader.doBulkLoad(new Path(args[1]), hTable);
			} else {
				System.out.println("loading failed.");
				System.exit(1);
			}

		} catch (IllegalArgumentException e) {
			// no region server? -> table does not exist.
			e.printStackTrace();
		} finally {
			if(hTable != null){
				hTable.close();
			}
		}
	}
}