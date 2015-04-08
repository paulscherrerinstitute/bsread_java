package ch.psi.bsread.configuration;

import java.util.List;

/**
 * Configuration of an BSREAD source. The specification can be found
 * at https://docs.google.com/document/d/1BynCjz5Ax-onDW0y8PVQnYmSssb6fAyHkdDl1zh21yY/edit#
 */
public class Configuration {
	
	private List<Channel> channels;

	public List<Channel> getChannels() {
		return channels;
	}

	public void setChannels(List<Channel> channels) {
		this.channels = channels;
	}
}
