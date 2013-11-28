package examples;

public class BSBMQueries {
	
	public static final String Q1 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/> "
			+"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/> "
			+"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			+"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "

			+"SELECT DISTINCT ?product ?label ?value1 "
			+"WHERE { "
			 +"?product rdfs:label ?label ."
			 +"?product a bsbm-inst:ProductType1 ."
			 +"?product bsbm:productFeature bsbm-inst:ProductFeature142 ." 
			 +"?product bsbm:productFeature bsbm-inst:ProductFeature144 ." 
			+"?product bsbm:productPropertyNumeric1 ?value1 ." 
				+"FILTER (?value1 > 20)} " 
			+"ORDER BY ?label "
			+"LIMIT 10";
	
	public static final String Q12 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>"
			+"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>"
			+"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
			+"PREFIX dc: <http://purl.org/dc/elements/1.1/>"
			+"PREFIX fromProducer1:  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/>"
			+"SELECT ?p ?producer" 
			+"WHERE {"
				+"fromProducer1:Product1 bsbm:producer ?p ."
				+"?p rdfs:label ?producer ."
			    +"fromProducer1:Product1 dc:publisher ?p . "
			+"}";

	public static final String Q2 = "PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>"
+"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>"
+"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
+"PREFIX dc: <http://purl.org/dc/elements/1.1/>"
+"PREFIX fromProducer1:  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/>"
+"SELECT ?label ?comment ?producer ?f ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3"
+" ?propertyNumeric1 ?propertyNumeric2" 
//+"SELECT * "
+"WHERE {"
 +"fromProducer1:Product1 rdfs:label ?label ."
	+"fromProducer1:Product1 rdfs:comment ?comment ."
	+"fromProducer1:Product1 bsbm:producer ?p ."
	+"?p rdfs:label ?producer ."
    +"fromProducer1:Product1 dc:publisher ?p . "
	+"fromProducer1:Product1 bsbm:productFeature ?f ."
	+"?f rdfs:label ?productFeature ."
	+"fromProducer1:Product1 bsbm:productPropertyTextual1 ?propertyTextual1 ."
	+"fromProducer1:Product1 bsbm:productPropertyTextual2 ?propertyTextual2 ."	
    +"fromProducer1:Product1 bsbm:productPropertyTextual3 ?propertyTextual3 ."
	+"fromProducer1:Product1 bsbm:productPropertyNumeric1 ?propertyNumeric1 ."
	+"fromProducer1:Product1 bsbm:productPropertyNumeric2 ?propertyNumeric2 ."
	//+"FILTER( 831 = ?propertyNumeric1 )"	
	+"OPTIONAL { fromProducer1:Product1 bsbm:productPropertyTextual4 ?propertyTextual4 }"
 +"OPTIONAL { fromProducer1:Product1 bsbm:productPropertyTextual5 ?propertyTextual5 }"
 +"OPTIONAL { fromProducer1:Product1 bsbm:productPropertyNumeric4 ?propertyNumeric4 }"
+"}";
	
	public static final String Q9 = "PREFIX rev: <http://purl.org/stuff/rev#>  " +
			"DESCRIBE ?x " +
			"WHERE { " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite1/Review1> rev:reviewer ?x " +
			"}";
	
	public static final String Q5 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
			"PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>  " +
			"SELECT DISTINCT ?product ?productLabel WHERE {          " +
			"?product rdfs:label ?productLabel .     " +
			"FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer494/Product24290> != ?product)   " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer494/Product24290> " +
			"		bsbm:productFeature ?prodFeature .   " +
			"?product bsbm:productFeature ?prodFeature .   " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer494/Product24290> " +
			"		bsbm:productPropertyNumeric1 ?origProperty1 .     " +
			"?product bsbm:productPropertyNumeric1 ?simProperty1 .   " +
			"FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))     " +
			"<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer494/Product24290> " +
			"		bsbm:productPropertyNumeric2 ?origProperty2 .     " +
			"?product bsbm:productPropertyNumeric2 ?simProperty2 .   " +
			"FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170)) " +
			"} " +
			"ORDER BY ?productLabel LIMIT 5";
	
	public static final String Q08 = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+
"SELECT ?var3 ?var6 WHERE { "+
"?var4 rdfs:label \"Climate forcing\"@en ."+
"?var3 skos:broader ?var4 ."+
"?var3 rdfs:label ?var6 ."+
"FILTER langMatches( lang(?var6), 'en') }";
}
