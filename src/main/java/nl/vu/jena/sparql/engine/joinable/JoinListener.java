package nl.vu.jena.sparql.engine.joinable;

import java.util.EventListener;


public interface JoinListener extends EventListener {
	
	public void joinFinished(JoinEvent e);

}
