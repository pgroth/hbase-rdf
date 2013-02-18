package nl.vu.jena.sparql.engine.optimizer.reorder;

import org.openjena.atlas.io.IndentedWriter;
import org.openjena.atlas.io.PrintUtils;
import org.openjena.atlas.io.Printable;

import com.hp.hpl.jena.sparql.sse.Item;

public class Pattern implements Printable {
	Item subjItem;
	Item predItem;
	Item objItem;
	double weight;
	boolean filtered = false;

	public Pattern(double w, Item subj, Item pred, Item obj) {
		weight = w;
		subjItem = subj;
		predItem = pred;
		objItem = obj;
	}
	
	public Pattern(double w, Item subj, Item pred, Item obj, boolean filtered) {
		weight = w;
		subjItem = subj;
		predItem = pred;
		objItem = obj;
		this.filtered = filtered;
	}

	@Override
	public String toString() {
		//return "("+subjItem+" "+predItem+" "+objItem+") ==> "+weight ;
		return PrintUtils.toString(this);
	}

	@Override
	public void output(IndentedWriter out) {
		out.print("(");
		out.print("(");
		out.print(subjItem.toString());
		out.print(" ");
		out.print(predItem.toString());
		out.print(" ");
		out.print(objItem.toString());
		out.print(")");
		out.print(" ");
		out.print(weight);
		out.print(")");
	}
}
