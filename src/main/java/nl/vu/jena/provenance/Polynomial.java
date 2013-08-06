package nl.vu.jena.provenance;

import java.util.ArrayList;

public class Polynomial {
	
	private ArrayList<PolynomialTerm> terms = new ArrayList<PolynomialTerm>();
	
	
	public Polynomial() {
		super();
	}

	public Polynomial(String first){
		terms.add(new PolynomialTerm(new PolynomialFactor(1, first)));
	}
	
	public Polynomial(PolynomialTerm first){
		terms.add(first);
	}
	
	/**
	 * Add another polynomial to this one
	 * Changes underlying polynomial
	 * @param other
	 */
	public Polynomial addPolynomial(Polynomial other){
		ArrayList<PolynomialTerm> first, second;
		ArrayList<PolynomialTerm> remainingFromFirst = new ArrayList<PolynomialTerm>();
		ArrayList<PolynomialTerm> remainingFromSecond = new ArrayList<PolynomialTerm>();
		if (this.terms.size() > other.terms.size()){
			first = this.terms;
			second = other.terms;
		}
		else{
			first = other.terms;
			second = this.terms;
		}
		
		Polynomial ret = new Polynomial();
		for (int i = 0; i < first.size(); i++) {
			for (int j = 0; j < second.size(); j++) {
				PolynomialTerm newTerm = first.get(i).addTerm(second.get(j));
				if (newTerm!=null){
					ret.terms.add(newTerm);
					break;
				}
				else{
					remainingFromFirst.add(first.get(i));
					remainingFromSecond.add(second.get(j));
				}
			}
		}
		
		if (second.isEmpty()){
			ret.terms.addAll(first);
		}
		else{
			ret.terms.addAll(remainingFromFirst);
			ret.terms.addAll(remainingFromSecond);
		}
		
		return ret;
	}
	
	/**
	 * Multiply this polynomial with another
	 * Changes underlying polynomial
	 * @param other
	 */
	public Polynomial multiplyWithPolynomial(Polynomial other){
		Polynomial result = new Polynomial();
		ArrayList<Polynomial> polynomials = new ArrayList<Polynomial>();
		
		for (PolynomialTerm term : terms) {
			Polynomial t = term.multiply(other);
			polynomials.add(t);
		}
		
		for (Polynomial polynomial : polynomials) {
			result = result.addPolynomial(polynomial);
		}
		
		return result;
	}
	
	/**
	 * Add a term to this polynomial
	 * Changes underlying polynomial
	 * @param otherTerm
	 */
	public void addTerm(PolynomialTerm otherTerm){
		boolean added=false;
		for (PolynomialTerm term : this.terms) {
			PolynomialTerm t = term.addTerm(otherTerm);
			if (t!=null){
				term.setFactors(t.getFactors());
				term.setCoefficient(t.getCoefficient());
				added=true;
				break;
			}
		}
		
		if (!added){
			terms.add(otherTerm);
		}
	}
	
	public String toString(){
		String ret = "";
		for (PolynomialTerm term : terms) {
			ret += "+"+term.toString();
		}
		return ret;
	}	

	public ArrayList<PolynomialTerm> getTerms() {
		return terms;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Polynomial a = new Polynomial("a");
		
		PolynomialTerm bTerm = new PolynomialTerm(new PolynomialFactor(1, "a"));
		bTerm.setTerm(bTerm.addTerm(new PolynomialTerm(new PolynomialFactor(1, "a"))));
		Polynomial b = new Polynomial(bTerm);
		
		Polynomial a1 = a.addPolynomial(b);
		System.out.print("("+a1+")*("+b+")=");
		Polynomial c = a1.multiplyWithPolynomial(b);
		System.out.println(c);

	}

}
