package translate.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.json.JSONArray;

import redis.clients.jedis.Jedis;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.expr.Expr;

import main.ShardedRedisTripleStoreV1;

public class ReducePhaseProject extends RedisProjectOP {
	String projectScript;
	
	public ReducePhaseProject(RedisOP _parent){
		super(_parent);
	}
	
	@Override
	public String mapLuaScript() {
		return parent.mapLuaScript();
	}


	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		QueryResult raw = parent.reduce(patternStack);
		
		List<Integer> projectedVarIndexes = new ArrayList<Integer>();
		for(String s: projectedVariables){
			boolean found = false;
			for(int varIdx = 0; varIdx < raw.columnNames.size(); varIdx++){
				if(s.equals(raw.columnNames.get(varIdx))){
					projectedVarIndexes.add(varIdx);
					found = true;
					break;
				}
				
			}
			if(!found){
				projectedVarIndexes.add(-1);
			}
		}
		QueryResult projected = new QueryResult(projectedVariables);
		for(List<Node> rawRow: raw.rows){
			List<Node> projectedRow = new ArrayList<Node>(rawRow.size());
			for(Integer varIdx: projectedVarIndexes){
				if(varIdx != -1){
					projectedRow.add(rawRow.get(varIdx));
				} else {
					projectedRow.add(Node.createLiteral(""));
				}
			}
			projected.addRow(projectedRow);
		}
		
		return projected;
	}
	
	@Override
	public Boolean completeAfterMapPhase() {
		return false;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("ReducePhaseProject {\n");
		sb.append(indent + "  projectVars: {");
		String delimiter = "";
		for(String projectedVariable: projectedVariables){
			sb.append(delimiter + "'" + projectedVariable + "'");
			delimiter = ",";
		}
		sb.append("}\n");
		sb.append(indent + "  parent : " + parent.toString(indent + "  "));
		return sb.toString();
	}



}
