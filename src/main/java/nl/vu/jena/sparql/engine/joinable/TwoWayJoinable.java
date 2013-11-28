package nl.vu.jena.sparql.engine.joinable;


public interface TwoWayJoinable extends Joinable {

	public Joinable getLeftJ();
	
	public Joinable getRightJ();
	
}
