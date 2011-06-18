package it.unina.tools.datastore;

public class ByteUtil {
	
		
		public static byte[] charsToBytes(char[] chars){
			byte[] bytes= new byte[chars.length];
	    	for(int i=0; i< chars.length; i++){
	    		bytes[i]=(byte)chars[i];
	    	}
	    	return bytes;
		}
		
		public static char[] bytesToChars(byte[] bytes){
			char[] chars= new char[bytes.length];
	    	for(int i=0; i< bytes.length; i++){
	    		chars[i]=(char)bytes[i];
	    	}
	    	return chars;
		}

	}
