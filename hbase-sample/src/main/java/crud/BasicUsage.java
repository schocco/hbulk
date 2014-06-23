package crud;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class BasicUsage {
	
	private static Configuration conf = HBaseConfiguration.create();
	private static final String TABLE_NAME = "t1";
	private static HTable htable;
	
	
	public static void main(String[] args) throws IOException{
		htable = createTable();
		try{
			insert();
			scanEnron();
			get();
		} finally {
			htable.close();
		}

	}
	
	/**
	 * Creates a new table is it does not exist yet.
	 * @return an HTable object
	 * @throws IOException
	 */
	private static HTable createTable() throws IOException{
		// create table for testing
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(!admin.isTableAvailable(TABLE_NAME.getBytes())){
			HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
			HColumnDescriptor meta = new HColumnDescriptor("vals".getBytes());
			HColumnDescriptor body = new HColumnDescriptor("aggr".getBytes());
			desc.addFamily(meta);
			desc.addFamily(body);
			admin.createTable(desc);
		}
		admin.close();
		return new HTable(conf, TABLE_NAME);
	}
	
	private static void insert() throws RetriesExhaustedWithDetailsException, InterruptedIOException{
		Put put = new Put("row1".getBytes());
		put.add("vals".getBytes(), "a".getBytes(), Bytes.toBytes(120));
		put.add("vals".getBytes(), "b".getBytes(), Bytes.toBytes(23));
		htable.put(put);
		System.out.println(htable.isAutoFlush());
	}
	
	
	private static void scanEnron() throws IOException{
		HTable enron = new HTable(conf, "enron");
		RegexStringComparator comp = new RegexStringComparator("(?i)^fw:.");
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
			"meta".getBytes(),	"Subject".getBytes(),	CompareOp.EQUAL,
			comp
			);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("meta"));
		scan.setFilter(filter);
		scan.setCaching(2000);
		ResultScanner scanner = enron.getScanner(scan);

		int count = 0;
		byte[] subject;
		for(Result res : scanner){
			count++;
			subject = res.getValue("meta".getBytes(), "Subject".getBytes());
			System.out.println(Bytes.toString(subject));
		}
		System.out.println(count);
		scanner.close();
		enron.close();
	}
	
	/**
	 * A simple get operation.
	 * @throws IOException
	 */
	private static void get() throws IOException{
		Get get = new Get("row1".getBytes());
		Result res = htable.get(get);
		byte[] a = res.getValue("vals".getBytes(), "a".getBytes());
		byte[] b = res.getValue("vals".getBytes(), "b".getBytes());
		System.out.println("vals:a is " + Bytes.toInt(a));
		System.out.println("vals:b is " + Bytes.toInt(b));
	}

}
