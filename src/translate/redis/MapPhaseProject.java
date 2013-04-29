package translate.redis;

import java.util.Stack;

public class MapPhaseProject extends RedisProjectOP {
	String projectScript;
	
	public MapPhaseProject(RedisOP _parent){
		super(_parent);
	}
	@Override
	public String mapLuaScript() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		String delimiter = "";
		for (String projectedVar : projectedVariables){
			sb.append(delimiter);
			sb.append("'" + projectedVar + "'");
			delimiter = ",";
		}
		sb.append("}");
		String result = ""
				+ parent.mapLuaScript()
				+ "  \n"
				+ "log(\"MapPhaseProject: " + sb.toString() + "\") \n"
				+ "local projectedVarNames = " + sb.toString() + " \n"
				+ "\n"
				+ "for i, graphPattern in ipairs(mapResults) do \n"
				+ "  -- figure out variable projection \n"
				+ "  local projectedVarIndexes = {} \n"
				+ "  for i,projectedVarName in ipairs(projectedVarNames) do \n"
				+ "    for j,outputVarName in ipairs(graphPattern[1]) do \n"
				+ "      if outputVarName == projectedVarName then \n"
				+ "        table.insert(projectedVarIndexes, j) \n"
				+ "      end \n"
				+ "    end \n"
				+ "  end \n"
				+ "  for j,outputVarName in ipairs(graphPattern[1]) do \n"
//				+ "    log('considering ' .. outputVarName) \n"
				+ "    if string.find(outputVarName, '^META_') then \n"
				+ "      table.insert(projectedVarIndexes, j) \n"
				+ "    end \n"
				+ "  end \n"
				+ "  -- do the projection \n"
				+ "  local newGraphPattern = {} \n"
				+ "  for i,row in ipairs(graphPattern) do \n"
				+ "    local newRow = {} \n"
				+ "    for i,projectedIdx in ipairs(projectedVarIndexes) do \n"
				+ "      table.insert(newRow, row[projectedIdx]) \n"
				+ "    end \n"
				+ "    table.insert(newGraphPattern, newRow) \n"
				+ "  end \n"
				+ "   \n"
				+ "  -- put the jsonified result table into graphPatternKey \n"
				+ "  mapResults[i] = newGraphPattern \n"
				+ "end \n"
				+ "";
		return result;
	}


	@Override
	public QueryResult reduce(Stack<QueryResult> patternStack) {
		return parent.reduce(patternStack);
	}
	
	@Override
	public Boolean completeAfterMapPhase() {
		return true;
	}

	@Override
	public String toString(String indent) {
		StringBuilder sb = new StringBuilder();
		sb.append("MapPhaseProject {\n");
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
