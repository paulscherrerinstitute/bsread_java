package ch.psi.daq.data.db.converters.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import ch.psi.daq.data.db.converters.AbstractByteArrayConverter;
import ch.psi.daq.data.stream.converter.BooleanByteValueConverter;
import ch.psi.daq.data.stream.converter.ByteValueConverter;

public class BooleanArrayByteConverter extends AbstractByteArrayConverter<boolean[], boolean[], Boolean> {
	private static final Logger LOGGER = Logger.getLogger(BooleanArrayByteConverter.class.getName());
	private static final BooleanByteValueConverter BYTE_BUFFER_CONVERTER = new BooleanByteValueConverter();

	@Override
	public boolean[] convert(ByteBuffer bytes, ByteOrder endianness) {
		boolean[] values = new boolean[bytes.remaining() / getNBytes()];
		this.convert(bytes, endianness, values);
		return values;
	}

	@Override
	public int convert(ByteBuffer bytes, ByteOrder endianness, boolean[] values) {
		// This code uses one byte per boolean (one byte could represent 8
		// booleans -> see BoolBooleanArrayByteConverter for such a memory
		// saving implementation). Using the "one byte per boolean"
		// representation allows an iteration over the elements by using the
		// ValueByteConverter (i.e. BooleanValueByteConverter) in conjunction
		// with Streams an thus, it is possible to handle all primitive types
		// with this concept. The waste of memory should (hopefully) be tackled
		// by compression.

		ByteBuffer buffer = bytes.order(endianness);
		int startPos = bytes.position();
		int nBytes = getNBytes();
		int length = Math.min(values.length, buffer.remaining() / nBytes);

		IntStream.range(0, length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				values[i] = BooleanByteValueConverter.convertVal(buffer.get(startPos + i * nBytes));
			}
		});

		return length;
	}

	@Override
	public ByteBuffer convertObject(Object value, ByteOrder endianness) {
		if (value instanceof boolean[]) {
			return this.convert((boolean[]) value, endianness);
		} else {
			String message = String.format("Could not cast '%s' into '%s'", value.getClass().getName(), boolean[].class.getName());
			LOGGER.log(Level.SEVERE, message);
			throw new RuntimeException(message);
		}
	}

	@Override
	public ByteBuffer convert(boolean[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	public static ByteBuffer convertBytes(boolean[] value, ByteOrder endianness) {
		ByteBuffer ret = ByteBuffer.allocate(value.length * getNBytes()).order(endianness);

		IntStream.range(0, value.length).parallel().forEach(new IntConsumer() {

			@Override
			public void accept(int i) {
				ret.put(i, BooleanByteValueConverter.convertVal(value[i]));
			}
		});

		// no flip necessary since we use absolut put()
		return ret;
	}

	@Override
	public int getBytes() {
		return getNBytes();
	}

	protected static int getNBytes() {
		return Byte.BYTES;
	}

	@Override
	public Class<boolean[]> getValueClass() {
		return boolean[].class;
	}

	@Override
	public Class<boolean[]> getSignedValueClass() {
		return this.getValueClass();
	}

	@Override
	public boolean[] convertAsSigned(ByteBuffer bytes, ByteOrder endianness) {
		return this.convert(bytes, endianness);
	}

	@Override
	public ByteBuffer convertAsSigned(boolean[] value, ByteOrder endianness) {
		return convertBytes(value, endianness);
	}

	@Override
	public int getArraySize(ByteBuffer bytes, ByteOrder endianness) throws UnsupportedOperationException {
		return bytes.remaining() / getBytes();
	}

	@Override
	public ByteValueConverter<Boolean> getByteValueConverter() {
		return BYTE_BUFFER_CONVERTER;
	}
}
