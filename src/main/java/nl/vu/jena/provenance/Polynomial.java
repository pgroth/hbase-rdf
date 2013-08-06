package nl.vu.jena.provenance;

import java.util.ArrayList;

public class Polynomial {
	
	private ArrayList<PolynomialTerm> terms = new ArrayList<PolynomialTerm>();
	
	public Polynomial(String first){
		terms.add(new PolynomialTerm(new PolynomialFactor(1, first)));
	}
	
	public Polynomial(PolynomialTerm first){
		terms.add(first);
	}
	
	public void add(Polynomial other){
		ArrayList<PolynomialTerm> remaining = new ArrayList<PolynomialTerm>();
		for (int i = 0; i < terms.size(); i++) {
			for (int j = 0; j < other.terms.size(); j++) {
				if (terms.get(i).addTerm(other.terms.get(j))){
					break;
				}
				else{
					remaining.add(other.terms.get(j));
				}
			}
		}
		
		terms.addAll(remaining);
	}
	
	public String toString(){
		String ret = "";
		for (PolynomialTerm term : terms) {
			ret += "+"+term.toString();
		}
		return ret;
	}	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Polynomial a = new Polynomial("a");
		PolynomialTerm bTerm = new PolynomialTerm(new PolynomialFactor(1, "a"));
		bTerm.addTerm(new PolynomialTerm(new PolynomialFactor(1, "a")));
		Polynomial b = new Polynomial(bTerm);
		
		a.add(b);
		System.out.println(a);

	}

}
