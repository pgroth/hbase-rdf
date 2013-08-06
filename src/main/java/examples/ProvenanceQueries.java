package examples;

public class ProvenanceQueries {
	
	public static final String Q1 = "prefix geo-ont: <http://www.geonames.org/ontology#>"+
	"prefix geo-countries: <http://www.geonames.org/countries/#>"+
	"prefix pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>"+
	"prefix dbpedia: <http://dbpedia.org/property/>"+
	"prefix dbpediares: <http://dbpedia.org/resource/>"+
	"prefix owl: <http://www.w3.org/2002/07/owl#>"+
	"select ?a ?b ?lat ?long"+ 
	"where {"+ 
	"?a ?b \"Eiffel Tower\"."+ 
	"?a geo-ont:inCountry geo-countries:FR."+ 
	"?a pos:lat ?lat."+
	"?a pos:long ?long. }";

}
