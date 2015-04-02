package ch.psi.bsread;

import java.util.concurrent.TimeUnit;

import ch.psi.daq.data.DBDataHelper;
import ch.psi.daq.data.DataType;

public interface BSReadConfig {
	// set low since images consume a lot of memory
//	public static final int HIGH_WATER_MARK = 10;
//	public static final long RECONNECT_SLEEP = TimeUnit.SECONDS.toMillis(1);
//
//	public static final String ZMQ_STREAM_PROTOCOL_DEFAULT = "tcp";
//	public static final int ZMQ_STREAM_PORT_DEFAULT = 9999;
//	
//	public static final String HEADER_MAIN_HTYPE_KEY = "htype";
//	public static final String HEADER_MAIN_HTYPE_VALUE = "bsr_m";
//	public static final String HEADER_MAIN_GLOBAL_TIME_KEY = "global-timestamp";
//	public static final String HEADER_MAIN_PULSE_ID_KEY = "pulse-id";
//	public static final String HEADER_MAIN_HASH_KEY = "hash";
//
//	public static final String HEADER_DATA_HTYPE_KEY = HEADER_MAIN_HTYPE_KEY;
//	public static final String HEADER_DATA_HTYPE_VALUE = "bsr_d";
//	public static final String HEADER_DATA_CHANNELS_CONFIG_KEY = "channels";
//	public static final String HEADER_DATA_CHANNELS_TYPE = "type";
//
//	public static final String HEADER_DATA_CHANNELS_TYPE_DEFAULT = DataType.TYPE_DOUBLE;
//	public static final int[] HEADER_DATA_CHANNELS_SHAPE_DEFAULT = DBDataHelper.SHAPE_INFO_SCALAR;
//	public static final String HEADER_DATA_CHANNELS_NAME = "name";
//	public static final String HEADER_DATA_CHANNELS_SHAPE = "shape";
//	public static final String HEADER_DATA_CHANNELS_ENCODING = "encoding";
//	public static final String HEADER_DATA_CHANNELS_ENCODING_BIG_ENDIAN = "big";
//	public static final String HEADER_DATA_CHANNELS_ENCODING_LITTLE_ENDIAN = "little";
//	public static final String HEADER_DATA_CHANNELS_FREQUENCY = "frequency";
//	public static final double HEADER_DATA_CHANNELS_FREQUENCY_DEFAULT = 100;
//	public static final String HEADER_DATA_CHANNELS_OFFSET = "offset";
//	public static final int HEADER_DATA_CHANNELS_OFFSET_DEFAULT = 0;
}
