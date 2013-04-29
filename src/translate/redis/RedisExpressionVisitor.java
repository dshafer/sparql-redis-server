package translate.redis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import main.ShardedRedisTripleStore;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.E_Lang;
import com.hp.hpl.jena.sparql.expr.E_LangMatches;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction0;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprFunction3;
import com.hp.hpl.jena.sparql.expr.ExprFunctionN;
import com.hp.hpl.jena.sparql.expr.ExprFunctionOp;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.ExprVisitor;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDecimal;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueDouble;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueFloat;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueInteger;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;

public class RedisExpressionVisitor implements ExprVisitor {
	ShardedRedisTripleStore ts;
	public Map<NodeValue, String> nodeAliases;
	public Set<Var> varsMentioned;
	public StringBuilder luaExpression;
	public Map<String, String> equalities;
	private Boolean hasNonEqualities;
	ExpressionOptimizer optimizer;
	public String getLuaFunctionExpression(){
		return luaExpression.toString();
	}
	public Set<Var> getVarsMentioned(){
		return varsMentioned;
	}
	public Map<String,String> getEqualities(){
		return equalities;
	}
	public Boolean hasNonEqualities(){
		return hasNonEqualities;
	}
	public RedisExpressionVisitor (ShardedRedisTripleStore _ts){
		luaExpression = new StringBuilder();
		optimizer = new ExpressionOptimizer();
		nodeAliases = new HashMap<NodeValue, String>();
		varsMentioned = new HashSet<Var>();
		equalities = new HashMap<String, String>();
		hasNonEqualities = false;
		ts = _ts;
	}

	@Override
	public void startVisit() {
		System.out.println("startVisit");
	}

	@Override
	public void visit(ExprFunction0 func) {
		System.out.println("ExprFunction0: " + func);
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(ExprFunction1 func) {
		System.out.println("ExprFunction1: " + func);
		String funcName = func.getFunctionName(null);

		
		if(funcName == "lang"){
			luaExpression.append("string.sub(");
			func.getArg().visit(this);
			luaExpression.append(",-4)");
		} else {
			throw new UnsupportedOperationException();
					
		}
	}
	
	private String getNodeValueAlias(NodeValue nv){
		String alias = nodeAliases.get(nv);
		if (alias == null){
			alias = ts.getAlias(nv.asNode());
			nodeAliases.put(nv, alias);
		}
		return alias;
	}

	@Override
	public void visit(ExprFunction2 func) {
		String opStr;
		if(func.isFunction()){
			if(func instanceof E_LangMatches){
				Expr arg1 = func.getArg1();
				Expr arg2 = func.getArg2();

				if((arg1 instanceof E_Lang) && (arg2 instanceof NodeValueString)){
					luaExpression.append("(");
					// target: (string.sub(vars['text'], -4) == '"@' .. string.lower('EN'))
					arg1.visit(this);
					luaExpression.append(" == ('\\\"@' .. string.lower(");
					arg2.visit(this);
					luaExpression.append(")))");
					hasNonEqualities = true;
				} else {
					throw new UnsupportedOperationException();
				}
				return;
			} else {
				throw new UnsupportedOperationException();
			}
		} else {
			opStr = func.getOpName();
		}
		
		Expr arg1 = func.getArg1();
		Expr arg2 = func.getArg2();
		Boolean numeric = false;
		Expr constArg = arg1.isConstant() ? arg1 : arg2;
		if(constArg.isConstant()){
			numeric |= (arg1 instanceof NodeValueDecimal) || (arg1 instanceof NodeValueDouble) || (arg1 instanceof NodeValueFloat) || (arg1 instanceof NodeValueInteger);
		}
		// special-case for equalities
		if(opStr == "="){
			if(arg1.isVariable() && arg2.isConstant()){
				equalities.put('?' + arg1.getVarName(), getNodeValueAlias(arg2.getConstant()));
			}
			numeric = false;
			opStr = "==";
		} else {
			hasNonEqualities = true;
			if(opStr == "!="){
				luaExpression.append(" not ");
				opStr = "==";
			} else if (opStr == "="){
				
			} else if (opStr == "||"){
				opStr = "or";
			}
		}
		luaExpression.append(" ( ");
		if(numeric){
			luaExpression.append("tonumber");
		}
		luaExpression.append("(");
		func.getArg1().visit(this);
		luaExpression.append(") ");
		luaExpression.append(opStr + " ");
		if(numeric){
			luaExpression.append("tonumber");
		}
		luaExpression.append("(");
		func.getArg2().visit(this);
		luaExpression.append(") ");
		luaExpression.append(" ) ");
	}

	@Override
	public void visit(ExprFunction3 func) {
		System.out.println("ExprFunction3: " + func);
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(ExprFunctionN func) {
		System.out.println("ExprFunctionN: " + func);
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(ExprFunctionOp funcOp) {
		System.out.println("ExprFunctionOp: " + funcOp);
		throw new UnsupportedOperationException();
	}

	@Override
	public void visit(NodeValue nv) {
		luaExpression.append('\'' + getNodeValueAlias(nv) + '\'');
	}

	@Override
	public void visit(ExprVar nv) {
		luaExpression.append("vars['?" + nv.getVarName() + "']");
		varsMentioned.add(nv.asVar());
	}

	@Override
	public void visit(ExprAggregator eAgg) {
		System.out.println("ExprAggregator: " + eAgg);
		throw new UnsupportedOperationException();
	}

	@Override
	public void finishVisit() {
		System.out.println("finishVisit");
		// TODO Auto-generated method stub
		
	}
	
}