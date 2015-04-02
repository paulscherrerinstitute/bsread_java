package ch.psi.bsread;

import static org.junit.Assert.*;

import org.junit.Test;

public class UtilsTest {

	@Test
	public void testComputeMD5() {
		String headerString = "{\"channels\": [{\"type\": \"double\", \"name\": \"CHANNEL-0\"}, {\"type\": \"double\", \"name\": \"CHANNEL-1\"}, {\"type\": \"double\", \"name\": \"CHANNEL-2\"}, {\"type\": \"double\", \"name\": \"CHANNEL-3\"}, {\"type\": \"string\", \"name\": \"CHANNEL-STRING\"}, {\"shape\": [2], \"type\": \"integer\", \"name\": \"CHANNEL-ARRAY_1\"}], \"htype\": \"bsr_d-1.0\", \"encoding\": \"little\"}";
		String md5 = Utils.computeMD5(headerString);
		System.out.println(md5);
		assertEquals("50acfbebaa30924c857740b5a4d770b5", md5);
	}

}
