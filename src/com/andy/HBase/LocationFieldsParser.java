package com.andy.HBase;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public class LocationFieldsParser {
	
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSSSSS");
	
	public static String  stringToFixLength(String str,int length)
	{
		while(str.length()<length)
		{
			
			str = str+"0";
		}
		
		if(str.length() > length)
		{
			str.substring(0, 15);
		}
		return str;
	}
	
	public static Long timeStrToLong(String time)
	{
		Long timestamp = 0L;
		if(!time.isEmpty())
		{
			try{
				Date date = df.parse(time);
				if(date != null)
				{
					timestamp = date.getTime();
				}
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		return timestamp;
	}
	public static void main(String[] args) {
//		try{
//		MessageDigest md = MessageDigest.getInstance("MD5");
//		long vid = Long.parseLong("3553831169700784");
//		md.update(Bytes.toBytes(vid));
//		byte[] bvid = md.digest();
//		long gps_ts = LocationFieldsParser.timeStrToLong(LocationFieldsParser.trim("2014-05-04-18.17.44.353933","\""));
//		byte[] result = concat(bvid,Bytes.toBytes(gps_ts));
//		}
//		catch(Exception e)
//		{
//			
//		}
		long vid = Long.parseLong("3553831169700784");
		byte[] bvid = Bytes.toBytes(vid);
		byte[] result = reverse(bvid);

		
		
		System.out.println(timeStrToLong("2011-10-24-16.30.28.000000"));
	}
	
	public static String trim(String source,String toTrim)
	{
		StringBuffer sb = new StringBuffer(source);
		while(toTrim.indexOf(new Character(sb.charAt(0)).toString()) != -1){
			sb.deleteCharAt(0);
		}
		while(toTrim.indexOf(new Character(sb.charAt(sb.length() - 1)).toString()) != -1)
		{
			sb.deleteCharAt(sb.length() -1);
		}
		return sb.toString();
	}
	
	public static byte[] concat(byte[] first,byte[] second)
	{
		byte[] result = Arrays.copyOf(first,first.length+second.length);
		System.arraycopy(second, 0, result, first.length, second.length);
		return result;
	}
	
	public static byte[] reverse(byte[] source)
	{
		byte[] result = new byte[source.length];
		for(int i = source.length-1;i>=0;i--)
		{
			result[(source.length -1) - i] = source[i];
		}
		return result;
	}
}