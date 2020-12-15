package utils;


public class Utils {

	/**
	 * if t is an application operation
	 * 
	 * @param operation
	 * @param c
	 * @return new operation
	 */
	public static byte[] joinByteArray(byte[] operation, char c) {

		byte[] result = new byte[operation.length + 2];

		System.arraycopy(operation, 0, result, 0, operation.length);
		System.arraycopy(c, 0, result, operation.length, 2);

		return result;
	}

	public static Operation splitByteArray(byte[] input) {

		byte[] operation = new byte[input.length - 2];
		byte[] c_bytes = new byte[2];
		System.arraycopy(input, 0, c_bytes, operation.length, input.length);

		String s = new String(c_bytes);
		char c = s.charAt(0);

		Operation op = new Operation(c, operation);
		return op;
	}
}
