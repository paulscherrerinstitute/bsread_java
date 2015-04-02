package ch.psi.bsread;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import ch.psi.bsread.message.ChannelConfig;
import ch.psi.bsread.message.DataHeader;
import ch.psi.daq.data.DataType;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BSReadHelper {
	private static Logger LOGGER = Logger.getLogger(BSReadHelper.class.getName());

	private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static final String ZMQ_PROTOCOL = "zmq.protocol";
	public static final String ZMQ_HOST = "zmq.host";
	public static final String ZMQ_PORT = "zmq.port";

	public static final int[] SHAPE_SCALAR = BSReadConfig.HEADER_DATA_CHANNELS_SHAPE_DEFAULT;
	public static final List<Integer> SHAPE_LIST_SCALAR = Arrays.asList(SHAPE_SCALAR[0]);

	/**
	 * Extract the shape info from a value.
	 * 
	 * @param value
	 *            The value
	 * @return List The shape info
	 */
	public static List<Integer> extractShapeAsList(Object value) {
		ArrayList<Integer> shapeList = new ArrayList<>();
		while (value != null && value.getClass().isArray()) {
			shapeList.add(Array.getLength(value));
			value = Array.get(value, 0);
		}

		if (!shapeList.isEmpty()) {
			return shapeList;
		} else {
			return SHAPE_LIST_SCALAR;
		}
	}

	/**
	 * Extract the shape info from a value.
	 * 
	 * @param value
	 *            The value
	 * @return int[] The shape info
	 */
	public static int[] extractShape(Object value) {
		List<Integer> shapeList = extractShapeAsList(value);

		if (shapeList != null && !shapeList.isEmpty()) {
			int[] shape = new int[shapeList.size()];
			for (int i = 0; i < shapeList.size(); ++i) {
				shape[i] = shapeList.get(i);
			}
			return shape;
		} else {
			return SHAPE_SCALAR;
		}
	}

	/**
	 * Calculates the array length of a shape
	 * 
	 * @param shape
	 *            The shape
	 * @return int The array length
	 */
	public static int getArrayLength(int[] shape) {
		if (shape != null && shape.length > 0) {
			int arrayLength = shape[0];
			for (int i = 1; i < arrayLength; ++i) {
				arrayLength *= shape[i];
			}
			return arrayLength;
		} else {
			return 1;
		}
	}

	/**
	 * Extends the DAQConfig with the ZMQ host and the ZMQ port.
	 * 
	 * @param daqConfig
	 *            The DAQConfig
	 * @param zmqHost
	 *            The ZMQ host
	 */
	public static void extendDAQConfig(DAQConfig daqConfig, String zmqHost) {
		daqConfig.setNonStandardParameter(ZMQ_HOST, zmqHost);
	}

	/**
	 * Extends the DAQConfig with the ZMQ host and the ZMQ port.
	 * 
	 * @param daqConfig
	 *            The DAQConfig
	 * @param zmqHost
	 *            The ZMQ host
	 * @param port
	 *            The ZMQ port
	 */
	public static void extendDAQConfig(DAQConfig daqConfig, String zmqHost, int port) {
		daqConfig.setNonStandardParameter(ZMQ_HOST, zmqHost);
		daqConfig.setNonStandardParameter(ZMQ_PORT, port);
	}

	/**
	 * Extends the DAQConfig with the ZMQ host and the ZMQ port.
	 * 
	 * @param daqConfig
	 *            The DAQConfig
	 * @param zmqHost
	 *            The ZMQ host
	 * @param protocol
	 *            The protocol (e.g. tcp)
	 * @param port
	 *            The ZMQ port
	 * @param configChannelName
	 *            The name of the config channel
	 */
	public static void extendDAQConfig(DAQConfig daqConfig, String zmqHost, String protocol, int port) {
		daqConfig.setNonStandardParameter(ZMQ_HOST, zmqHost);
		daqConfig.setNonStandardParameter(ZMQ_PROTOCOL, protocol);
		daqConfig.setNonStandardParameter(ZMQ_PORT, port);
	}

	/**
	 * Extracts the ZMQ source (protocol, host and port) from a DAQConfig.
	 * 
	 * @param daqConfig
	 *            The DAQConfig
	 * @return String The
	 */
	public static String getZMQStreamSource(DAQConfig daqConfig) {
		String zmqHost = daqConfig.getNonStandardParameter(ZMQ_HOST, null);
		if (zmqHost == null) {
			String message = String.format("There is no zmq host defined in '%s'", daqConfig.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}

		String protocol = daqConfig.getNonStandardParameter(ZMQ_PROTOCOL, BSReadConfig.ZMQ_STREAM_PROTOCOL_DEFAULT);
		int port = daqConfig.getNonStandardParameter(ZMQ_PORT, BSReadConfig.ZMQ_STREAM_PORT_DEFAULT);
		return protocol + "://" + zmqHost + ":" + port;
	}

	/**
	 * Ectracts the InputConfig based on the channel names provided in the
	 * DAQConfig.
	 * 
	 * @param daqConfig
	 *            The DAQConfig
	 * @return Collection The InputSourceConfigs
	 */
	public static Collection<InputSourceConfig> extractInputSourceConfigs(DAQConfig daqConfig) {
		String zmqStreamSource = BSReadHelper.getZMQStreamSource(daqConfig);

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket socket = context.socket(ZMQ.PULL);
		socket.setRcvHWM(1);
		socket.connect(zmqStreamSource);

		socket.recvStr(); // String mainHeaderStr
		String dataHeaderStr = socket.recvStr();

		BSReadHelper.drain(socket);
		socket.close();
		context.close();

		try {
			DataHeader dataHeaderConf = OBJECT_MAPPER.readValue(dataHeaderStr, DataHeader.class);
			return BSReadHelper.convertToInputSourceConfigs(dataHeaderConf);
		} catch (IOException e) {
			String message = String.format("Could not parse data header of config '%s'", daqConfig.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	/**
	 * Extracts the InputConfig based on the BSReadDataHeader.
	 * 
	 * @param dataHeaderConf
	 *            The BSReadDataHeader
	 * @return Collection The InputSourceConfigs
	 */
	public static Collection<InputSourceConfig> convertToInputSourceConfigs(DataHeader dataHeaderConf) {
		Class<?> valueClazz;
		double frequency;
		int arrayLength;
		List<ChannelConfig> channelConfs = dataHeaderConf.getChannels();
		Collection<InputSourceConfig> inSConfs = new ArrayList<>(channelConfs.size());
		for (ChannelConfig config : channelConfs) {
			valueClazz = DataType.getValueClass(config.getType());
			frequency = config.getFrequency();
			arrayLength = BSReadHelper.getArrayLength(config.getShape());

			inSConfs.add(new InputSourceConfig(config.getName(), valueClazz, arrayLength, InputSourceConfig.getUpdateRate(frequency)));
		}

		return inSConfs;
	}

	/**
	 * Drains a socket
	 * 
	 * @param socket
	 *            The socket
	 */
	public static void drain(Socket socket) {
		while (socket.hasReceiveMore()) {
			// is there a way to avoid copying data to user space here?
			socket.recv();
		}
	}

	// /**
	// * Extracts the type info of a value.
	// *
	// * @param value
	// * The value
	// * @return String The type
	// */
	// @Deprecated
	// public static String extractType(Object value) {
	// // LOGGER.log(Level.WARNING, ()->
	// // "TODO: Replace this code with call to ByteConverterProvider.");
	//
	// Class<?> valueClazz = value.getClass();
	//
	// while (valueClazz.isArray()) {
	// valueClazz = Array.get(value, 0).getClass();
	// }
	//
	// String type = valueClazz.getSimpleName();
	// if ("int".equals(type)) {
	// type = ByteConverterProvider.TYPE_INTEGER;
	// }
	//
	// return type;
	// }
}
