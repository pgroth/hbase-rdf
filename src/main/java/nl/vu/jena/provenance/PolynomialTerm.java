package nl.vu.jena.provenance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class PolynomialTerm {

	private int coefficient;
	private ArrayList<PolynomialFactor> factors = new ArrayList<PolynomialFactor>();

	public PolynomialTerm(PolynomialFactor factor) {
		this.factors.add(factor);
		coefficient = 1;
	}

	public boolean addTerm(PolynomialTerm other) {
		if (this.sameFactors(other)) {
			this.coefficient += other.coefficient;
			return true;
		} else {
			return false;
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
