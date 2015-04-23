package ch.psi.bsread.message;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

public class MainHeaderTest {

	@Test
	public void testDeserialization() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		MainHeader header = mapper.readValue(this.getClass().getResource("main_header.json").openStream(), MainHeader.class);
		
		assertEquals("50acfbebaa30924c857740b5a4d770b5", header.getHash());
		assertEquals("bsr_m-1.0", header.getHtype());
		assertEquals(0L, header.getPulseId());
		assertEquals(1427960013647L,header.getGlobalTimestamp().getEpoch());
		assertEquals(0L,header.getGlobalTimestamp().getNs());
	}
	
	@Test
	public void testSerialization() throws IOException {
		
		MainHeader header = new MainHeader();
		header.setHash("50acfbebaa30924c857740b5a4d770b5");
		header.setPulseId(0);
		header.setGlobalTimestamp(new Timestamp(1427960013647L, 0));
		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(header);
		String expected = "{\"htype\":\"bsr_m-1.0\",\"hash\":\"50acfbebaa30924c857740b5a4d770b5\",\"global_timestamp\":{\"epoch\":1427960013647,\"ns\":0}}";
		assertEquals(expected, json);
		
		System.out.println(json);
	}
}
