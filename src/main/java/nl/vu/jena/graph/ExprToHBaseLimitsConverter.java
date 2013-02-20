package nl.vu.jena.graph;

import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.retrieve.RowLimitPair;

import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_GreaterThan;
import com.hp.hpl.jena.sparql.expr.E_LessThan;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.NodeValue;

public class ExprToHBaseLimitsConverter {

	public static RowLimitPair getRowLimitPair(Expr e){
		RowLimitPair ret = null;
		if (!(e instanceof ExprFunction2)){
			throw new RuntimeException("Simple filters are expected to have 2 arguments");
		}
		
		if (e instanceof E_GreaterThan){
			E_GreaterThan gt = (E_GreaterThan)e;
			if (gt.getArg1().isConstant()){
				TypedId limitValue = convertToPrimaryType(gt.getArg1().getConstant());
				limitValue.adjustContent(-1);
				ret = new RowLimitPair(limitValue, RowLimitPair.END_LIMIT);
			}
			else if (gt.getArg2().isConstant()){
				TypedId limitValue = convertToPrimaryType(gt.getArg2().getConstant());
				limitValue.adjustContent(1);
				ret = new RowLimitPair(limitValue, RowLimitPair.START_LIMIT);
			}
		} 
		else if (e instanceof E_LessThan){
			E_LessThan lt = (E_LessThan)e;
			if (lt.getArg1().isConstant()){
				TypedId limitValue = convertToPrimaryType(lt.getArg1().getConstant());
				limitValue.adjustContent(1);
				ret = new RowLimitPair(limitValue, RowLimitPair.START_LIMIT);
			}
			else if (lt.getArg2().isConstant()){
				TypedId limitValue = convertToPrimaryType(lt.getArg2().getConstant());
				limitValue.adjustContent(-1);
				ret = new RowLimitPair(limitValue, RowLimitPair.END_LIMIT);
			}
		}
		else if (e instanceof E_Equals){
			E_Equals eq = (E_Equals)e;
			NodeValue constantNode = null;
			if (eq.getArg1().isConstant()){
				constantNode = eq.getArg1().getConstant();
			}
			else if (eq.getArg2().isConstant()){
				constantNode = eq.getArg2().getConstant();
			}
			
			TypedId limitValue = convertToPrimaryType(constantNode);
			return new RowLimitPair(limitValue, limitValue);
		}
		//TODO remaining
		
		return ret;
	}
	
	private static TypedId convertToPrimaryType(NodeValue constant){
		TypedId ret;
		if (constant.isDouble()){
			ret =  new TypedId(TypedId.XSD_DOUBLE, Bytes.toBytes(constant.getDouble()));
		}
		else if (constant.isInteger()){//TODO also check for long and BigInteger here
			ret =  new TypedId(TypedId.XSD_INT, Bytes.toBytes(constant.getInteger().intValue()));
		}
		else if (constant.isFloat()){
			ret =  new TypedId(TypedId.XSD_DOUBLE, Bytes.toBytes(constant.getFloat()));
		}
		else
			throw new RuntimeException("Unknown type in expression");
		
		//for now assume all values are positive 
		//TODO ret[0] = (byte)((~ret[0]&0x80) | (ret[0]&0x7f));//flip first bit so that 
													//numerical order coincides with lexicographical order
		return ret;
	}
}
