package nl.vu.jena.graph;

import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.parameters.RowLimitPair;

import org.apache.hadoop.hbase.util.Bytes;
import org.openjena.jenasesame.impl.Convert;
import org.openrdf.model.Literal;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_GreaterThan;
import com.hp.hpl.jena.sparql.expr.E_GreaterThanOrEqual;
import com.hp.hpl.jena.sparql.expr.E_LessThan;
import com.hp.hpl.jena.sparql.expr.E_LessThanOrEqual;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.NodeValue;

public class ExprToHBaseLimitsConverter {

	public static RowLimitPair getRowLimitPair(Expr e) throws Exception {
		RowLimitPair ret = null;
		if (!(e instanceof ExprFunction2)){
			throw new RuntimeException("Simple filters are expected to have 2 arguments");
		}

		if (e instanceof E_GreaterThan) {
			E_GreaterThan gt = (E_GreaterThan) e;
			if (gt.getArg1().isConstant()) {
				TypedId limitValue = convertNumericToPrimaryType(gt.getArg1().getConstant());
				limitValue.adjustContent(-1);
				ret = new RowLimitPair(limitValue, RowLimitPair.END_LIMIT);
			} else if (gt.getArg2().isConstant()) {
				TypedId limitValue = convertNumericToPrimaryType(gt.getArg2().getConstant());
				limitValue.adjustContent(1);
				ret = new RowLimitPair(limitValue, RowLimitPair.START_LIMIT);
			}
		} else if (e instanceof E_GreaterThanOrEqual) {
			E_GreaterThanOrEqual gte= (E_GreaterThanOrEqual) e;
			if (gte.getArg1().isConstant()) {
				TypedId limitValue = convertNumericToPrimaryType(gte.getArg1().getConstant());
				ret = new RowLimitPair(limitValue, RowLimitPair.END_LIMIT);
			} else if (gte.getArg2().isConstant()) {
				TypedId limitValue = convertNumericToPrimaryType(gte.getArg2().getConstant());
				ret = new RowLimitPair(limitValue, RowLimitPair.START_LIMIT);
			}
		} else if (e instanceof E_LessThan) {
			E_LessThan lt = (E_LessThan) e;
			if (lt.getArg1().isConstant()) {
				TypedId limitValue = convertNumericToPrimaryType(lt.getArg1().getConstant());
				limitValue.adjustContent(1);
				ret = new RowLimitPair(limitValue, RowLimitPair.START_LIMIT);
			} else if (lt.getArg2().isConstant()) {
				TypedId limitValue = convertNumericToPrimaryType(lt.getArg2().getConstant());
				limitValue.adjustContent(-1);
				ret = new RowLimitPair(limitValue, RowLimitPair.END_LIMIT);
			}
		} else if (e instanceof E_LessThanOrEqual){
			E_LessThanOrEqual lte = (E_LessThanOrEqual) e;
			if (lte.getArg1().isConstant()) {
				TypedId limitValue = convertNumericToPrimaryType(lte.getArg1().getConstant());
				ret = new RowLimitPair(limitValue, RowLimitPair.START_LIMIT);
			} else if (lte.getArg2().isConstant()) {
				TypedId limitValue = convertNumericToPrimaryType(lte.getArg2().getConstant());
				ret = new RowLimitPair(limitValue, RowLimitPair.END_LIMIT);
			}
		} 
		else if (e instanceof E_Equals) {
			E_Equals eq = (E_Equals) e;
			NodeValue constantNode = null;
			if (eq.getArg1().isConstant()) {
				constantNode = eq.getArg1().getConstant();
			} else if (eq.getArg2().isConstant()) {
				constantNode = eq.getArg2().getConstant();
			}

			if (constantNode.isNumber()){
				TypedId limitValue = convertNumericToPrimaryType(constantNode);
				return new RowLimitPair(limitValue, limitValue);
			}
			else
				throw new RuntimeException("Unsupported Simple Filter: "+eq.getOpName());//TODO
		}
		//TODO remaining
		
		return ret;
	}
	
	private static TypedId convertNumericToPrimaryType(NodeValue constant) throws Exception{
		TypedId ret;
		byte []backingArray = new byte[TypedId.SIZE];
		if (constant.isInteger()){
			Bytes.putInt(backingArray, TypedId.SIZE-Integer.SIZE/8, constant.getInteger().intValue());
			ret =  new TypedId(TypedId.XSD_INTEGER, backingArray);
		}
		else if (constant.isDecimal()){
			ret = TypedId.createNumerical((Literal)Convert.nodeLiteralToValue(new ValueFactoryImpl(), constant.asNode()));
		}
		else if (constant.isFloat()){
			Bytes.putFloat(backingArray, TypedId.SIZE-Float.SIZE/8, constant.getFloat());
			ret =  new TypedId(TypedId.XSD_DOUBLE, backingArray);
		}
		else if (constant.isDouble()){
			Bytes.putDouble(backingArray, TypedId.SIZE-Double.SIZE/8, constant.getDouble());
			ret =  new TypedId(TypedId.XSD_DOUBLE, backingArray);
		}
		else
			throw new RuntimeException("Unknown type in expression");
		
		//for now assume all values are positive 
		//TODO ret[0] = (byte)((~ret[0]&0x80) | (ret[0]&0x7f));//flip first bit so that 
													//numerical order coincides with lexicographical order
		return ret;
	}
}
