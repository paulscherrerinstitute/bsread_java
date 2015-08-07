package ch.psi.bsread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.bsread.message.MainHeader;
import ch.psi.bsread.message.Message;
import ch.psi.bsread.message.Timestamp;
import ch.psi.bsread.message.Value;

public class Receiver {
	private static Logger LOGGER = Logger.getLogger(Receiver.class.getName());
	public static final int HIGH_WATER_MARK = 100;

	private Context context;
	private Socket socket;

	private ObjectMapper mapper = new ObjectMapper();

	private List<Consumer<MainHeader>> mainHeaderHandlers = new ArrayList<>();
	private List<Consumer<DataHeader>> dataHeaderHandlers = new ArrayList<>();
	private List<Consumer<Map<String, Value>>> valueHandlers = new ArrayList<>();
	private boolean parallelProcessing = false;

	private String dataHeaderHash = "";
	private DataHeader dataHeader = null;

	public void connect() {
		connect("tcp://localhost:9999");
	}

	public void connect(String address) {
		this.connect(address, HIGH_WATER_MARK);
	}
	
	public void connect(String address, int highWaterMark) {
		this.context = ZMQ.context(1);
		this.socket = this.context.socket(ZMQ.PULL);
		this.socket.setRcvHWM(highWaterMark);
		this.socket.connect(address);
	}

	public void close() {
		socket.close();
		context.close();
		socket = null;
		context = null;
	}

	public Message receive() throws RuntimeException {
		try {
			// Receive main header
			MainHeader mainHeader = mapper.readValue(socket.recv(), MainHeader.class);
			if (!mainHeader.getHtype().startsWith(MainHeader.HTYPE_VALUE_NO_VERSION)) {
				String message = String.format("Expect 'bsr_d-[version]' for 'htype' but was '%s'. Skip messge", mainHeader.getHtype());
				LOGGER.log(Level.SEVERE, message);
				this.drain();
				throw new RuntimeException(message);
			}

			if (this.parallelProcessing) {
				mainHeaderHandlers.parallelStream().forEach(handler -> handler.accept(mainHeader));
			} else {
				mainHeaderHandlers.forEach(handler -> handler.accept(mainHeader));
			}

			// Receive data header
			if (socket.hasReceiveMore()) {
				if (mainHeader.getHash().equals(dataHeaderHash)) {
					// The data header did not change so no interpretation of
					// the header ...
					socket.recv();
				}
				else {
					dataHeaderHash = mainHeader.getHash();
					dataHeader = mapper.readValue(socket.recv(), DataHeader.class);
					if (this.parallelProcessing) {
						dataHeaderHandlers.parallelStream().forEach(handler -> handler.accept(dataHeader));
					} else {
						dataHeaderHandlers.forEach(handler -> handler.accept(dataHeader));
					}
				}
			}
			else {
				String message = "There is no data header. Skip complete message.";
				LOGGER.log(Level.SEVERE, message);
				this.drain();
				throw new RuntimeException(message);
			}

			// Receiver data
			Message message = new Message();
			message.setMainHeader(mainHeader);
			message.setDataHeader(dataHeader);
			Map<String, Value> values = new HashMap<>();
			message.setValues(values);
			List<ChannelConfig> channelConfigs = dataHeader.getChannels();
			int i = 0;
			for (; i < channelConfigs.size() && this.socket.hasReceiveMore(); ++i) {
				ChannelConfig currentConfig = channelConfigs.get(i);

				// # read data blob #
				// ##################
				if (!this.socket.hasReceiveMore()) {
					final String errorMessage = String.format("There is no data for channel '%s'.", currentConfig.getName());
					LOGGER.log(Level.WARNING, errorMessage);
					throw new RuntimeException(errorMessage);
				}
				byte[] valueBytes = socket.recv(); // value

				// # read timestamp blob #
				// #######################
				if (!this.socket.hasReceiveMore()) {
					final String errorMessage = String.format("There is no timestamp for channel '%s'.", currentConfig.getName());
					LOGGER.log(Level.WARNING, errorMessage);
					throw new RuntimeException(errorMessage);
				}
				byte[] timestampBytes = socket.recv();

				// Create value object
				if (valueBytes != null && valueBytes.length > 0) {
					Value value = new Value();

					// TODO always convert to BigEndian byte order!
					// Why? Leads to unnecessary conversion work during DAQ.
					// Fabian objects to this TODO (or make it configurable with
					// default "non conversion")
					value.setValue(valueBytes);
					ByteBuffer tsByteBuffer = ByteBuffer.wrap(timestampBytes).order(currentConfig.getByteOrder());
					// c-implementation uses a unsigned long (Json::UInt64, uint64_t) for time -> decided to ignore this here
					value.setTimestamp(new Timestamp(tsByteBuffer.getLong(), tsByteBuffer.getLong()));
					values.put(currentConfig.getName(), value);
				}
			}

			// Sanity check of value list
			if (i != channelConfigs.size()) {
				LOGGER.log(Level.WARNING, "Number of received values does not match number of channels.");
			}
			if (this.socket.hasReceiveMore()) {
				// Some sender implementations add an empty additional message
				// at the end
				// If there is more than 1 trailing message something is wrong!
				int messagesDrained = this.drain();
				if (messagesDrained > 1) {
					throw new RuntimeException("There were more than 1 trailing submessages to the message than expected");
				}
			}

			// notify hooks with complete values
			if (!values.isEmpty()) {
				if (this.parallelProcessing) {
					valueHandlers.parallelStream().forEach(handler -> handler.accept(values));
				}
				else {
					valueHandlers.forEach(handler -> handler.accept(values));
				}
			}

			return message;

		} catch (IOException e) {
			throw new RuntimeException("Unable to deserialize message", e);
		}
	}

	public int drain() {
		int count = 0;
		while (socket.hasReceiveMore()) {
			// is there a way to avoid copying data to user space here?
			socket.recv();
			count++;
		}
		return count;
	}

	public void addValueHandler(Consumer<Map<String, Value>> handler) {
		valueHandlers.add(handler);
	}

	public void removeValueHandler(Consumer<Map<String, Value>> handler) {
		valueHandlers.remove(handler);
	}

	public void addMainHeaderHandler(Consumer<MainHeader> handler) {
		mainHeaderHandlers.add(handler);
	}

	public void removeMainHeaderHandler(Consumer<MainHeader> handler) {
		mainHeaderHandlers.remove(handler);
	}

	public void addDataHeaderHandler(Consumer<DataHeader> handler) {
		dataHeaderHandlers.add(handler);
	}

	public void removeDataHeaderHandler(Consumer<DataHeader> handler) {
		dataHeaderHandlers.remove(handler);
	}

	public void setParallelProcessing(boolean parallelProcessing) {
		this.parallelProcessing = parallelProcessing;
	}

	public boolean isParallelProcessing() {
		return this.parallelProcessing;
	}
}
