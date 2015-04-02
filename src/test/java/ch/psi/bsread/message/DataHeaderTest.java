package ch.psi.bsread.message;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DataHeaderTest {

	@Test
	public void test() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		DataHeader header = mapper.readValue(this.getClass().getResource("data_header.json").openStream(), DataHeader.class);
		
		String json = mapper.writeValueAsString(header);
		System.out.println(json);
	}

}
