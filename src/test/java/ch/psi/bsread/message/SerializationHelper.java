package ch.psi.bsread.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SerializationHelper {

	@SuppressWarnings("unchecked")
	public static <T> T copy(T object) throws Exception {
		// Serialization of object
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(bos);
		out.writeObject(object);

		// De-serialization of object
		ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
		ObjectInputStream in = new ObjectInputStream(bis);
		T copied = (T) in.readObject();

		out.close();
		in.close();

		return copied;
	}
}
