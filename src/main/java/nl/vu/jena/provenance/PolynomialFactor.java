package nl.vu.jena.provenance;


public class PolynomialFactor {
	
	private int exponent;
	private String var;

	public PolynomialFactor(int exponent, String var) {
		super();
		this.exponent = exponent;
		this.var = var;
	}

	public void incExponent() {
		exponent++;
	}

	public int getExponent() {
		return exponent;
	}

	public String getVar() {
		return var;
	}

	@Override
	public boolean equals(Object obj) {
		PolynomialFactor other = (PolynomialFactor) obj;
		return (other.getExponent() == exponent && other.getVar().equals(var));
	}
	
	public String toString(){
		return var+"^"+exponent;
	}
	
}
