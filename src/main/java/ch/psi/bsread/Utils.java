package ch.psi.bsread;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
	
	/**
	 * Generate the hexadecimal MD5 sum string of a string
	 * @param string	String to compute MD5 sum of
	 * @return	MD5 sum of string in hex format
	 */
	public static String computeMD5(String string){
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			StringBuffer hexString = new StringBuffer();
			for (byte b : md.digest(string.getBytes())) {
	            hexString.append(Integer.toHexString(0xFF & b));
			}
	        return hexString.toString();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
