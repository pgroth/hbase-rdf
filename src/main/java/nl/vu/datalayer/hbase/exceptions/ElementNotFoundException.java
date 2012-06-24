package nl.vu.datalayer.hbase.exceptions;

public class ElementNotFoundException extends Exception {
	static final long serialVersionUID = 2L;

	public ElementNotFoundException() {
		super();
	}

	public ElementNotFoundException(String message) {
		super(message);
	}
}
