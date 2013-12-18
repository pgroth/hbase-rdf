package nl.vu.jena.sparql.engine.joinable;

import java.util.EventObject;

public class JoinEvent extends EventObject {

	private static final long serialVersionUID = 1L;

	public JoinEvent(Object source) {
		super(source);
	}

}
