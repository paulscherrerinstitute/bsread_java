package ch.psi.bsread.configuration;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConfigurationTest {

	@Test
	public void test() throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		Configuration configuration = mapper.readValue(Configuration.class.getResourceAsStream("configuration.json"), Configuration.class);
		
//		configuration.getChannels().stream()
//			.forEach(channel -> System.out.println(channel.getName()));
		
		long count = configuration.getChannels().stream()
			.count();
		
		assertEquals(2, count);
	}

}
