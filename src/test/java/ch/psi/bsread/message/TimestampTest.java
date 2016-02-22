package ch.psi.bsread.message;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimestampTest {

	@Test
	public void testTimestamp_01() throws Exception {
		Timestamp time = Timestamp.ofMillis(1234);
		
		assertEquals(1, time.getSec());
		assertEquals(234000000, time.getNs());
	}

//	@Test
//	public void testTimestamp_02() throws Exception {
//		double delta = 0.00000000000000000001;
//
//		Timestamp time = new Timestamp(12, 500500000);
//		assertEquals(12, time.getSec());
//		assertEquals(500500000, time.getNs());
//		assertEquals(12500, time.getMillis());
//		assertEquals(0.5, time.getMillisFractional(), delta);
//
//		time = new Timestamp(34545, 999524075);
//		assertEquals(34545, time.getSec());
//		assertEquals(999524075, time.getNs());
//		assertEquals(34545999, time.getMillis());
//		assertEquals(0.524075, time.getMillisFractional(), delta);
//
//		time = new Timestamp(34545, 999524280);
//		assertEquals(34545, time.getSec());
//		assertEquals(999524280, time.getNs());
//		assertEquals(34545999, time.getMillis());
//		assertEquals(0.524280, time.getMillisFractional(), delta);
//
//		time = new Timestamp(34545, 999524283);
//		assertEquals(34545, time.getSec());
//		assertEquals(999524283, time.getNs());
//		assertEquals(34545999, time.getMillis());
//		assertEquals(0.524283, time.getMillisFractional(), delta);
//
//		time = new Timestamp(34545, 999524286);
//		assertEquals(34545, time.getSec());
//		assertEquals(999524286, time.getNs());
//		assertEquals(34545999, time.getMillis());
//		assertEquals(0.524286, time.getMillisFractional(), delta);
//
//		time = new Timestamp(34545, 999524287);
//		assertEquals(34545, time.getSec());
//		assertEquals(999524287, time.getNs());
//		assertEquals(34545999, time.getMillis());
//		assertEquals(0.524287, time.getMillisFractional(), delta);
//	}

}
