package nl.vu.datalayer.hbase.sail;

public class QueryExpansionException extends Exception {
    public QueryExpansionException(String string) {
        super(string);
    }
    
    public QueryExpansionException(String string, Exception ex) {
        super(string, ex);
    }
}
