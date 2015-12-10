package ch.psi.bsread.message.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.psi.bsread.IReceiver;
import ch.psi.bsread.command.AbstractCommand;
import ch.psi.bsread.message.Message;

public class ReconnectCommand extends AbstractCommand {
	private static final long serialVersionUID = -1681049107646307776L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectCommand.class);

	public static final String HTYPE_VALUE_NO_VERSION = "bsr_reconnect";
	// update AbstractCommand when version increases to support old and new
	// Command
	public static final String DEFAULT_HTYPE = HTYPE_VALUE_NO_VERSION + "-1.0";

	private String htype = DEFAULT_HTYPE;
	private String address;

	public ReconnectCommand() {
	}

	public ReconnectCommand(String address) {
		this.address = address;
	}

	public String getHtype() {
		return htype;
	}

	public void setHtype(String htype) {
		this.htype = htype;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	@Override
	public <V> Message<V> process(IReceiver<V> receiver) {
		LOGGER.info("Reconnect to '{}'", address);

		receiver.close();
		receiver.connect(this.address);

		return null;
	}

}
