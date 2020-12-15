package utils;

public class Operation {

	private char c;
	private byte[] operation;

	public Operation(char c, byte[] operation) {
		this.c = c;
		this.operation = operation;
	}

	public char getC() {
		return c;
	}

	public void setC(char c) {
		this.c = c;
	}

	public byte[] getOperation() {
		return operation;
	}

	public void setOperation(byte[] operation) {
		this.operation = operation;
	}
}
