package translate.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Variable;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.Binding0;
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.NodeValue;

public class ReducePhaseFilter implements RedisOP{

	RedisOP parent;
	Expr expr;
	public ReducePhaseFilter(RedisOP _parent, Expr _expr){
		parent = _parent;
		expr = _expr;
		Binding b;
	}
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();
	}

	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		
		QueryResult pre = parent.reduce(patternStack);
		long startTime = System.currentTimeMillis();
		QueryResult result = new QueryResult(pre.columnNames);
		
		List<Var> vars = new ArrayList<Var>();
		for(String colName : pre.columnNames){
			
			vars.add(Var.alloc(colName.substring(1)));
		}
		for(List<Node> row : pre.rows){
			BindingHashMap bm = new BindingHashMap();
			for(int v=0; v < vars.size(); v++){
				bm.add(vars.get(v), row.get(v));
			}
			if(expr.eval(bm, null) == NodeValue.TRUE){
				result.addRow(row);
			}
			
		}
		System.out.println("ReducePhaseFilter: " + (double)(System.currentTimeMillis() - startTime)/1000 + " seconds");
		return result;
	}

	@Override
	public Boolean completeAfterMapPhase() {
		return false;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("ReducePhaseFilter {\n");
		sb.append(indent + "  expr  : " + this.expr.toString() + "\n");
		sb.append(indent + "  parent: " + this.parent.toString(indent + "  ") + "\n");
		sb.append(indent + "}");
		return sb.toString();
	}

	@Override
	public Boolean tryAddFilter(RedisExpressionVisitor rev) {
		return parent.tryAddFilter(rev);
	}

	@Override
	public RedisOP tryConvertToJoin(RedisOP op, Boolean left) {
		RedisOP attempt = parent.tryConvertToJoin(op, left);
		if(attempt != null){
			this.parent = attempt;
			return this;
		}
		return null;
	}

	@Override
	public Set<String> getJoinVariables() {
		return parent.getJoinVariables();
	}

}
