package nl.vu.datalayer.hbase.exceptions;

public class NonNumericalException extends Exception {

	public NonNumericalException() {
		super();
	}

	public NonNumericalException(String message, Throwable cause) {
		super(message, cause);
	}

	public NonNumericalException(String message) {
		super(message);
	}

	public NonNumericalException(Throwable cause) {
		super(cause);
	}
	
}
