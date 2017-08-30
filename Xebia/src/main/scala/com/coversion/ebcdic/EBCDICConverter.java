package com.coversion.ebcdic;

import java.io.File;
//import java.nio.charset.Charset;
//import java.util.Iterator;
//import java.util.Set;
//import java.util.SortedMap;

public class EBCDICConverter {

	public static void main(String[] args) throws Exception {
		FileConverter fc = new FileConverter();
		String ebcdicFile = "C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\EBCDIC.txt";
		String oupputfile = "C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\output\\output.txt";
//		
//		SortedMap<String, Charset> m = Charset.availableCharsets();
//		Set<String> k = m.keySet();
//		Iterator<String> i = k.iterator();
//		while (i.hasNext()) {
//			String charsetTemp = i.next();
//			//System.out.println(charsetTemp);
//			
//		}
//		
		fc.setEbcdicCharset("IBM1047");
		fc.convert( new File(ebcdicFile) , new File(oupputfile));
		//IBM1026
		//fc.close(fc.writer);
		//fc.convert( new File(ebcdicFile) , new File(oupputfile));
	}
}
