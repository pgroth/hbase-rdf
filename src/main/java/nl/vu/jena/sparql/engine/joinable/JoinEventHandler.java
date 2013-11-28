package nl.vu.jena.sparql.engine.joinable;

import java.util.HashSet;
import java.util.concurrent.ExecutorService;


/**
 * Acts as a Join EventSource for the listeners at the upper levels
 * Acts as a listener of JoinEvents from lowever levels
 */
public class JoinEventHandler {

	private Joinable joinable;
	private int j = 2;
	private ExecutorService executor;
	
	private HashSet<JoinListener> listeners;
	
	public JoinEventHandler(ExecutorService executor, Joinable joinable) {
		super();
		this.joinable = joinable;
		this.executor = executor;
		listeners = new HashSet<JoinListener>();
	}

	public void registerListener(JoinListener listener){
		listeners.add(listener);
	}
	
	public void notifyListeners(){
		for (JoinListener listener : listeners) {
			listener.joinFinished(new JoinEvent(joinable));
		}
	}


	public synchronized void joinFinished(JoinEvent e) {
		j--;
		if (j <= 0){
			executor.execute(joinable);
		}
	}
}
