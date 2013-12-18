package nl.vu.jena.sparql.engine.joinable;

import java.util.Set;

public interface Joinable extends Runnable{
	
	public Set<String> getVarNames();
	
	public void setParent(JoinListener parent);

}
