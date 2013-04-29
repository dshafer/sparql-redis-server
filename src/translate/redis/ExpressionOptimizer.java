package translate.redis;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.expr.E_Equals;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.ExprTransform;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

public class ExpressionOptimizer implements ExprTransform{

	
	public Expr optimized(Expr e){
		if(e instanceof ExprFunction0){
			return transform((ExprFunction0)e);
		} else if (e instanceof ExprFunction1){
			ExprFunction1 func = (ExprFunction1)e;
			return transform(func, func.getArg());
		} else if (e instanceof ExprFunction2){
			ExprFunction2 func = (ExprFunction2)e;
			return transform(func, func.getArg1(), func.getArg2());
		} else if (e instanceof ExprFunction3){
			ExprFunction3 func = (ExprFunction3)e;
			return transform(func, func.getArg1(), func.getArg2(), func.getArg3());
		} else if (e instanceof ExprFunctionN){
			ExprFunctionN func = (ExprFunctionN)e;
			return transform(func, new ExprList(func.getArgs()));
		} else if (e instanceof ExprFunctionOp){
			throw new UnsupportedOperationException();
		} else if (e instanceof NodeValue) {
			return transform((NodeValue)e);
		} else if (e instanceof ExprVar) {
			return transform((ExprVar)e);
		} else if (e instanceof ExprAggregator) {
			return transform((ExprAggregator)e);
		} else {
			return e;
		}
	}
	@Override
	public Expr transform(ExprFunction0 func) {
		return func;
	}

	@Override
	public Expr transform(ExprFunction1 func, Expr expr1) {
		System.out.println("Optimizing ExprFunction1: " + func);
		String funcName = func.getFunctionName(null);
		if(funcName.equals("bound") && expr1.isVariable()){
			ExprVar ev = (ExprVar) expr1;
			return new E_NotEquals(ev, new NodeValueString("@"));
		} else if (funcName.equals("not")){
			Expr oArg = optimized(expr1);
			if(oArg instanceof E_NotEquals){
				E_NotEquals eNE = (E_NotEquals)oArg;
				return new E_Equals(eNE.getArg1(), eNE.getArg2());
			} else {
				return new E_LogicalNot(oArg);
			}
		}

		return func;
	}

	@Override
	public Expr transform(ExprFunction2 func, Expr expr1, Expr expr2) {
		return func;
	}

	@Override
	public Expr transform(ExprFunction3 func, Expr expr1, Expr expr2, Expr expr3) {
		return func;
	}

	@Override
	public Expr transform(ExprFunctionN func, ExprList args) {
		return func;
	}

	@Override
	public Expr transform(ExprFunctionOp funcOp, ExprList args, Op opArg) {
		return funcOp;
	}

	@Override
	public Expr transform(NodeValue nv) {
		return nv;
	}

	@Override
	public Expr transform(ExprVar nv) {
		return nv;
	}

	@Override
	public Expr transform(ExprAggregator eAgg) {
		return eAgg;
	}


}
