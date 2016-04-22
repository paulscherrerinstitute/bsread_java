package ch.psi.bsread.message.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;

import ch.psi.bsread.ConfigIReceiver;
import ch.psi.bsread.command.Command;
import ch.psi.bsread.message.Message;

public class StopCommand implements Command {
	private static final long serialVersionUID = 552749029819075031L;
	private static final Logger LOGGER = LoggerFactory.getLogger(StopCommand.class);

	public static final String HTYPE_VALUE_NO_VERSION = "bsr_stop";
	// update AbstractCommand when version increases to support old and new
	// Command
	public static final String DEFAULT_HTYPE = HTYPE_VALUE_NO_VERSION + "-1.0";

	@JsonInclude
	private String htype = DEFAULT_HTYPE;

	public StopCommand() {
	}

	public String getHtype() {
		return htype;
	}

	public void setHtype(String htype) {
		this.htype = htype;
	}

	@Override
	public <V> Message<V> process(ConfigIReceiver<V> receiver) {
		if (receiver.getReceiverConfig().isKeepListeningOnStop()) {
			receiver.drain();
			return null;
		} else {
			LOGGER.info("Stop receiving.");
			receiver.close();
			return null;
		}
	}
}
