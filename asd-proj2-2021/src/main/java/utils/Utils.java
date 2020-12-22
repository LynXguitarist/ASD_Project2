package utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class Utils {

	/**
	 * if t is an application operation
	 * 
	 * @param operation
	 * @param c
	 * @return new operation
	 */
	public static byte[] joinByteArray(byte[] operation, char c) {

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {
			output.write(operation);
			output.write(c);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return output.toByteArray();
	}

	public static Operation splitByteArray(byte[] input) {

		byte[] operation = new byte[input.length - 1];
		byte[] c_bytes = new byte[1];
		System.arraycopy(input,  operation.length, c_bytes,0, 1);

		String s = new String(c_bytes);
		char c = s.charAt(0);
		if (c != 'a' && c != 's')
			operation = input;

		Operation op = new Operation(c, operation);
		return op;
	}

	public static byte[] convertToBytes(Object object) throws IOException {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
			out.writeObject(object);
			return bos.toByteArray();
		}
	}

	public static Object convertFromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes); ObjectInput in = new ObjectInputStream(bis)) {
			return in.readObject();
		}
	}
}
