package nl.vu.jena.provenance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class PolynomialTerm {

	private int coefficient;
	private ArrayList<PolynomialFactor> factors = new ArrayList<PolynomialFactor>();

	public PolynomialTerm() {
		super();
		coefficient = 1;
	}

	public PolynomialTerm(PolynomialFactor factor) {
		this.factors.add(factor);
		coefficient = 1;
	}

	public PolynomialTerm addTerm(PolynomialTerm other) {
		if (this.sameFactors(other)) {
			PolynomialTerm ret = new PolynomialTerm();
			ret.coefficient = this.coefficient + other.coefficient;
			ret.factors = this.factors;
			return ret;
		} else {
			return null;
		}
	}

	private boolean sameFactors(PolynomialTerm other) {
		if (factors.size() != other.factors.size()) {
			return false;
		}
		for (int i = 0; i < factors.size(); i++) {
			if (!factors.get(i).equals(other.factors.get(i)))
				return false;
		}

		return true;
	}
	
	public Polynomial multiply(Polynomial polynomial) {
		Polynomial ret = new Polynomial();
		
		for (PolynomialTerm term : polynomial.getTerms()) {
			ret.addTerm(this.multiplyWith(term));
		}
		
		return ret;
	}
	
	public PolynomialTerm multiplyWith(PolynomialTerm other){
		PolynomialTerm ret = new PolynomialTerm();
		ret.coefficient = this.coefficient*other.coefficient;
		
		ret.setFactors(this.factors);
		for (PolynomialFactor factor : other.factors) {
			ret.multiplyWith(factor);
		}
		
		return ret;
	}

	
	public int getCoefficient() {
		return coefficient;
	}

	public void setCoefficient(int coefficient) {
		this.coefficient = coefficient;
	}

	public ArrayList<PolynomialFactor> getFactors() {
		return factors;
	}

	public void setFactors(ArrayList<PolynomialFactor> factors) {
		this.factors = factors;
	}
	
	public void setTerm(PolynomialTerm t){
		this.factors = t.getFactors();
		this.coefficient = t.getCoefficient();
	}

	public void multiplyWith(PolynomialFactor factor) {
		boolean found = false;
		for (PolynomialFactor f : factors) {
			if (f.getVar().equals(factor.getVar())) {
				f.incExponent();
				found = true;
				break;
			}
		}

		if (!found) {
			factors.add(factor);
			Collections.sort(factors, new Comparator<PolynomialFactor>() {
				@Override
				public int compare(PolynomialFactor o1, PolynomialFactor o2) {
					if (o1.getExponent() < o2.getExponent())
						return -1;
					else if (o1.getExponent() > o2.getExponent())
						return 1;
					else
						return o1.getVar().compareTo(o2.getVar());
				}

			});
		}
	}
	
	public String toString(){
		String ret= ""+coefficient;
		for (PolynomialFactor f : factors) {
			ret += "*"+f.toString();
		}
		return ret;
	}

}
