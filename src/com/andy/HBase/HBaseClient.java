package com.andy.HBase;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.index.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.IndexedHTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseClient {
	
	private static Configuration conf = null;
	static{ 
		conf = HBaseConfiguration.create();
		conf.set("dfs.replication", "1");
//		conf.set("hbase.rootdir", "hdfs://192.168.52.130:9000/hbase");
//		conf.set("hbase.zookeeper.quorum", "192.168.52.131,192.168.52.132,192.168.52.133");
//		conf.set("hbase.master", "192.168.52.130:60000");
	}
	public static void main(String[] args) {
		
		String tablename = "location_test";
		try{
//			HBaseClient.get(tablename,"130601900244_1406093583_andy");
//			HBaseClient.scan(tablename);
			boolean result = deleteTable(tablename);
			createTableWithIndex(tablename,new String(Bytes.toBytes((byte)12)),new String(Bytes.toBytes((byte)22)));
//			String str = new String(Bytes.toBytes((byte)12));
//			boolean isCreated = createTableWithOneIndex(tablename,new String(Bytes.toBytes((byte)12)));
//			System.out.println("create table success!");
//			boolean isInsearted = indexTest(tablename);
//			System.out.println(isInsearted);
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	

//	public static void scan(String tablename) throws Exception{
//		
//		HTable table = new HTable(conf,tablename);
//		Scan s = new Scan();
//		ResultScanner rs = table.getScanner(s);
//
//		for(Result r:rs)
//		{
//			CellScanner cs = r.cellScanner();
//			while(cs.advance()){
//				Cell cell = cs.current();
//				System.out.print(new String(CellUtil.cloneRow(cell))+" ");
//				System.out.print(new String(CellUtil.cloneFamily(cell))+":");
//				System.out.print(new String(CellUtil.cloneQualifier(cell))+" ");
//				System.out.print(cell.getTimestamp()+" ");
//				System.out.println(new String(CellUtil.cloneValue(cell)));
//			}
//		}
//		table.close();
//	}
	public static void get(String table,String row) throws Exception
	{
		HTable htable = new HTable(conf,table);
		Get get = new Get(Bytes.toBytes(row));
		Result result = htable.get(get);
		System.out.println("Get: "+result);
		htable.close();
	}
	
	public static boolean createTableWithIndex(String tableName,String indexQualifierOne,String indexQualifierTwo)
	{
		try{
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if(hBaseAdmin.tableExists(tableName)){
				System.out.println(tableName+" is already exists!");
				return false;
			}
			IndexedHTableDescriptor htd = new IndexedHTableDescriptor(tableName); 
			HColumnDescriptor hcd = new HColumnDescriptor("f");
			hcd.setCompressionType(Algorithm.SNAPPY);
			hcd.setMaxVersions(1);
			htd.addFamily(hcd);
			IndexSpecification ispOne = new IndexSpecification("gps_time_idx");
			ispOne.addIndexColumn(hcd, indexQualifierOne, ValueType.Long, 8);
			IndexSpecification ipsTwo = new IndexSpecification("geohash_idx");
			ipsTwo.addIndexColumn(hcd, indexQualifierTwo, ValueType.Long, 8);
			htd.addIndex(ispOne);
			htd.addIndex(ipsTwo);
			byte[] startKey = new byte[16];
			byte[] endKey = new byte[16];
			for(int i = 0; i< endKey.length ;i++){
				endKey[i] = (byte)0xFF;
			}
			hBaseAdmin.createTable(htd,startKey,endKey,32);
			return true;
			}
			catch(Exception e){
				e.printStackTrace();
				return false;
			}
		
	}
	
	public static boolean deleteTable(String tableName)
	{
		try{
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			return true;
		}catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
		
	}
	
	public static boolean indexTest(String tableName)
	{
		HTablePool pool = new HTablePool(conf,100);
		HTableInterface  table = pool.getTable(tableName);
		Put put = new Put("1234561211111112".getBytes());
		put.add("f".getBytes(),Bytes.toBytes((byte)12),"aaaa".getBytes());
		try{
			table.put(put);
		}catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static boolean createTableWithOneIndex(String tableName,String indexQualifier)
	{
		try{
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if(hBaseAdmin.tableExists(tableName)){
				System.out.println(tableName+" is already exists!");
				return false;
			}
			IndexedHTableDescriptor htd = new IndexedHTableDescriptor(tableName); 
			HColumnDescriptor hcd = new HColumnDescriptor("f");
			hcd.setCompressionType(Algorithm.SNAPPY);
			hcd.setMaxVersions(1);
			htd.addFamily(hcd);
			IndexSpecification ispOne = new IndexSpecification("name_idx");
			ispOne.addIndexColumn(hcd, indexQualifier, ValueType.String, 20);
			htd.addIndex(ispOne);
			hBaseAdmin.createTable(htd);
			return true;
			}
			catch(Exception e){
				e.printStackTrace();
				return false;
			}
		
	}
}
