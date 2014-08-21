package com.andy.HBase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePerformanceTest {

	protected static final Log LOG = LogFactory
			.getLog(HBasePerformanceTest.class.getName());

	private static final int ROW_LENGTH = 1000;
	private static final int ONE_GB = 1024 * 1024 * 1000;
	private static final int ROWS_PER_GB = ONE_GB / ROW_LENGTH;

	public static final byte[] TABLE_NAME = Bytes.toBytes("location_test");
	public static final byte[] FAMILY_NAME = Bytes.toBytes("f");
	// geohash
	public static final byte[] QUALIFIER_NAME = Bytes.toBytes((byte) 22);

	volatile Configuration conf;
	private int numClients = 1;
	private boolean flushCommits = true;
	private boolean writeToWAL = true;
	public static ArrayList<byte[]> keys = new ArrayList<byte[]>();
	public static int totalRows;

	static {
		CsvUtil csvUtil = new CsvUtil(
				"E:\\ДњТы\\java\\eclipse_fo_java\\HBaseClient\\conf\\keys.txt");
		csvUtil.fileKeysList(keys);
		totalRows = keys.size();
	}

	protected HTableDescriptor getTableDescriptor() {
		HTableDescriptor hd = new HTableDescriptor(TABLE_NAME);
		hd.addFamily(new HColumnDescriptor(FAMILY_NAME));
		return hd;
	}

	public HBasePerformanceTest(final Configuration c, int num) {
		this.conf = c;
		this.numClients = num;
	}

	public HBasePerformanceTest(final Configuration c) {
		this.conf = c;
	}

	static interface Status {
		/**
		 * Sets status
		 * 
		 * @param msg
		 *            status message
		 * @throws IOException
		 */
		void setStatus(final String msg) throws IOException;
	}

	static class CsvUtil {
		private String filename = null;

		public CsvUtil(String path) {
			this.filename = path;
		}

		public boolean fileKeysList(List<byte[]> keys) {
			BufferedReader br;
			try {
				br = new BufferedReader(new FileReader(filename));

				String row;
				while ((row = br.readLine()) != null) {
					String[] data_arr = row.split(",");
					String vehicle_id = data_arr[1];
					long vehicle = 0;
					if (!vehicle_id.isEmpty() && vehicle_id != null) {
						vehicle = Long.parseLong(vehicle_id);
					}
					byte[] bve = LocationFieldsParser.reverse(Bytes
							.toBytes(vehicle));
					long gps_ts = Long.MAX_VALUE
							- LocationFieldsParser
									.timeStrToLong(LocationFieldsParser.trim(
											data_arr[12], "\""));
					byte[] bgps = Bytes.toBytes(gps_ts);
					byte[] bRowKey = LocationFieldsParser.concat(bve, bgps);
					keys.add(bRowKey);
				}
			} catch (Exception e) {
				return false;
			}
			return true;
		}

	}

	static class TestOptions {
		private int startRow;
		private int perClientRunRows;
		private int totalRows;
		private byte[] tableName;
		private boolean flushCommits;
		private boolean writeToWAL = true;
		private int numClients;

		TestOptions() {
		}

		TestOptions(int startRow, int numClients, byte[] tableName,
				boolean flushCommits, boolean writeToWAL) {
			this.startRow = startRow;
			this.numClients = numClients;
			this.tableName = tableName;
			this.flushCommits = flushCommits;
			this.writeToWAL = writeToWAL;
		}

		public int getStartRow() {
			return startRow;
		}

		public int getPerClientRunRows() {
			return perClientRunRows;
		}

		public int getTotalRows() {
			return totalRows;
		}

		public byte[] getTableName() {
			return tableName;
		}

		public boolean isFlushCommits() {
			return flushCommits;
		}

		public boolean isWriteToWAL() {
			return writeToWAL;
		}

		public int getNumClients() {
			return numClients;
		}
	}

	static abstract class Test {
		// Below is make it so when Tests are all running in the one
		// jvm, that they each have a differently seeded Random.
		private static final Random randomSeed = new Random(
				System.currentTimeMillis());

		private static long nextRandomSeed() {
			return randomSeed.nextLong();
		}

		protected final Random rand = new Random(nextRandomSeed());

		protected final int startRow;
		protected int perClientRunRows;
		protected int totalRows;
		private final Status status;
		protected byte[] tableName;
		protected HBaseAdmin admin;
		protected HTable table;
		protected volatile Configuration conf;
		protected boolean flushCommits;
		protected boolean writeToWAL;
		protected int numClients;

		/**
		 * Note that all subclasses of this class must provide a public
		 * contructor that has the exact same list of arguments.
		 */
		Test(final Configuration conf, final TestOptions options,
				final Status status) {
			super();

			this.startRow = options.getStartRow();
			this.perClientRunRows = options.getPerClientRunRows();
			this.totalRows = options.getTotalRows();
			this.status = status;
			this.tableName = options.getTableName();
			this.table = null;
			this.conf = conf;
			this.flushCommits = options.isFlushCommits();
			this.writeToWAL = options.isWriteToWAL();
			this.numClients = options.getNumClients();
		}

		private String generateStatus(final int sr, final int i, final int lr) {
			return sr + "/" + i + "/" + lr;
		}

		protected int getReportingPeriod() {
			int period = this.perClientRunRows / 10;
			return period == 0 ? this.perClientRunRows : period;
		}

		void testSetup() throws IOException {
			this.totalRows = keys.size();
			this.perClientRunRows = this.totalRows / this.numClients;
			this.admin = new HBaseAdmin(conf);
			this.table = new HTable(conf, tableName);
			this.table.setAutoFlush(false);
			this.table.setScannerCaching(30);
		}

		void testTakedown() throws IOException {
			if (flushCommits) {
				this.table.flushCommits();
			}
			table.close();
		}

		/*
		 * Run test
		 * 
		 * @return Elapsed time.
		 * 
		 * @throws IOException
		 */
		long test() throws IOException {
			long elapsedTime;
			testSetup();
			long startTime = System.currentTimeMillis();
			try {
				testTimed();
				elapsedTime = System.currentTimeMillis() - startTime;
			} finally {
				testTakedown();
			}
			return elapsedTime;
		}

		/**
		 * Provides an extension point for tests that don't want a per row
		 * invocation.
		 */
		void testTimed() throws IOException {
			int lastRow = this.startRow * this.perClientRunRows
					+ this.perClientRunRows;
			// Report on completion of 1/10th of total.
			for (int i = this.startRow * this.perClientRunRows; i < lastRow; i++) {
				testRow(i);
				if (status != null && i > 0 && (i % getReportingPeriod()) == 0) {
					status.setStatus(generateStatus(this.startRow
							* this.perClientRunRows, i, lastRow));
				}
			}
		}

		/*
		 * Test for individual row.
		 * 
		 * @param i Row index.
		 */
		void testRow(final int i) throws IOException {
		}
	}

	long runOneClient(final Class<? extends Test> cmd, final int startRow,
			final int numClients, boolean flushCommits, boolean writeToWAL,
			final Status status) throws IOException {
		status.setStatus("Start " + cmd + " at offset " + startRow
				* (totalRows / numClients) + " for " + totalRows / numClients
				+ " rows");
		long totalElapsedTime = 0;

		Test t = null;
		TestOptions options = new TestOptions(startRow, numClients,
				getTableDescriptor().getName(), flushCommits, writeToWAL);
		try {
			Constructor<? extends Test> constructor = cmd
					.getDeclaredConstructor(Configuration.class,
							TestOptions.class, Status.class);
			t = constructor.newInstance(this.conf, options, status);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException("Invalid command class: "
					+ cmd.getName()
					+ ".  It does not provide a constructor as described by"
					+ "the javadoc comment.  Available constructors are: "
					+ Arrays.toString(cmd.getConstructors()));
		} catch (Exception e) {
			throw new IllegalStateException(
					"Failed to construct command class", e);
		}
		totalElapsedTime = t.test();

		status.setStatus("Finished " + cmd + " in " + totalElapsedTime
				+ "ms at offset " + startRow * (totalRows / numClients));
		return totalElapsedTime;
	}

	private void doMultipleClients(final Class<? extends Test> cmd)
			throws IOException {
		final List<Thread> threads = new ArrayList<Thread>(this.numClients);
		for (int i = 0; i < this.numClients; i++) {
			Thread t = new Thread(Integer.toString(i)) {
				@Override
				public void run() {
					super.run();
					HBasePerformanceTest pe = new HBasePerformanceTest(conf);
					int index = Integer.parseInt(getName());
					try {
						long elapsedTime = pe.runOneClient(cmd, index,
								numClients, flushCommits, writeToWAL,
								new Status() {
									public void setStatus(final String msg)
											throws IOException {
										LOG.info("client-" + getName() + " "
												+ msg);
									}
								});
						LOG.info("Finished " + getName() + " in " + elapsedTime
								+ "ms!");
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			};
			threads.add(t);
		}
		for (Thread t : threads) {
			t.start();
		}
		for (Thread t : threads) {
			while (t.isAlive()) {
				try {
					t.join();
				} catch (InterruptedException e) {
					LOG.debug("Interrupted, continuing" + e.toString());
				}
			}
		}
	}

	static class ReadTest extends Test {
		ReadTest(Configuration conf, TestOptions options, Status status) {
			super(conf, options, status);
		}

		@Override
		void testRow(final int i) throws IOException {
			Get get = new Get(HBasePerformanceTest.keys.get(i));
			get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
			Result result = this.table.get(get);
			if (!result.isEmpty()) {
				System.out.println(Bytes.toLong(result.getValue(FAMILY_NAME,
						QUALIFIER_NAME)));

			}
		}

		@Override
		protected int getReportingPeriod() {
			int period = this.perClientRunRows / 100;
			return period == 0 ? this.perClientRunRows : period;
		}

	}

	public static void main(final String[] args) {
		Configuration c = HBaseConfiguration.create();
		try {
			new HBasePerformanceTest(c, 10).doMultipleClients(ReadTest.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
