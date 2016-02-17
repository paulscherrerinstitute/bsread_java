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
		assertEquals("bsr_m-1.1", header.getHtype());
		assertEquals(0L, header.getPulseId());
		assertEquals(1427960013L, header.getGlobalTimestamp().getSec());
		assertEquals(647000543L, header.getGlobalTimestamp().getNs());
	}

	@Test
	public void testSerialization() throws IOException {

		MainHeader header = new MainHeader();
		header.setHash("50acfbebaa30924c857740b5a4d770b5");
		header.setPulseId(1);
		header.setGlobalTimestamp(new Timestamp(1427960013L, 647000468));

		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(header);
		String expected = "{\"htype\":\"bsr_m-1.1\",\"hash\":\"50acfbebaa30924c857740b5a4d770b5\",\"pulse_id\":1,\"global_timestamp\":{\"sec\":1427960013,\"ns\":647000468}}";
		assertEquals(expected, json);

		System.out.println(json);
	}
}
