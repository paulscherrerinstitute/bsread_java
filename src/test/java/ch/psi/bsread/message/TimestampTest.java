package ch.psi.bsread.message;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimestampTest {

	@Test
	public void testTimestamp_01() throws Exception {
		Timestamp time = Timestamp.ofMillis(1234);
		
		assertEquals(1, time.getSec());
		assertEquals(234000000, time.getNs());
		assertEquals(1234, time.getAsMillis());
	}
}
