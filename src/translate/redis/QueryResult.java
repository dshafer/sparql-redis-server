package translate.redis;

import java.util.ArrayList;
import java.util.List;

import main.ShardedRedisTripleStore;

import org.json.JSONArray;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_NULL;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.QuerySolutionMap;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.binding.BindingHashMap;

public class QueryResult implements ResultSet{
    
	public List<String> columnNames;
	public List<List<String>> rows;
	Boolean initialized;
	
	public QueryResult(){
		initialized = false;
		rows = new ArrayList<List<String>>();
	}
	
	public QueryResult(List<String> _columnNames){
		columnNames = _columnNames;
		rows = new ArrayList<List<String>>();
		initialized = true;
	}
	
	public void addRow(List<String> row){
		rows.add(row);
	}
	
	private ArrayList<String> JSONArrayToArrayList(JSONArray ja){
		ArrayList<String> result = new ArrayList<String>(ja.length());
		for(int i = 0; i < ja.length(); i++){
			result.add(ja.getString(i));
		}
		return result;
	}
	
	public void append(QueryResult from){
		rows.addAll(from.rows);
	}

	public void addPatternFromJSON(String patternJSON) {
		JSONArray data = new JSONArray(patternJSON);
		if (!initialized){
			columnNames = JSONArrayToArrayList(data.getJSONArray(0));
			initialized = true;
		} else {
			// validate that column names match previous
			JSONArray jColNames = data.getJSONArray(0);
			if (jColNames.length() != columnNames.size()){
				throw new IllegalArgumentException();
			}
			for(int i = 0; i < jColNames.length(); i++){
				if (!jColNames.getString(i).equals(columnNames.get(i))){
					throw new IllegalArgumentException();
				}
			}
		}
		
		for(int r = 1; r < data.length(); r++){
			rows.add(JSONArrayToArrayList(data.getJSONArray(r)));
		}
	}
	
	
    private static String center(String s, int size, String pad) {
        if (pad == null)
            throw new NullPointerException("pad cannot be null");
        if (pad.length() <= 0)
            throw new IllegalArgumentException("pad cannot be empty");
        if(s == null){
        	s = "null";
        }
        if (size <= s.length())
            return s;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (size - s.length()) / 2; i++) {
            sb.append(pad);
        }
        sb.append(s);
        while (sb.length() < size) {
            sb.append(pad);
        }
        return sb.toString();
    }
    
    private String nodeVal(Node n){
    	if(n.isLiteral()){
    		return n.getLiteralLexicalForm();
    	} else if(n.isURI()){
    		return n.getURI();
    	} else if(n instanceof Node_NULL){
    		return "null";
    	} else {
    		return n.toString();
    	}
    }
    
	public String asTable(){
		StringBuilder sb = new StringBuilder();
		int numCols = 0;
		for(String colName : columnNames){
			if(!colName.startsWith("META_")){
				numCols++;
			}
		}
		List<Integer> colWidths = new ArrayList<Integer>(numCols);
		for(int c = 0; c < numCols; c++){
			colWidths.add(columnNames.get(c).length());
		}
		for(List<Node> row:nodeRows){
			for(int c = 0; c < numCols; c++){
				int len = nodeVal(row.get(c)).length();
				if ((row.get(c) != null) && (colWidths.get(c) < len)){
					colWidths.set(c, len);
				}
			}
		}
		for(int c = 0; c < numCols; c++){
			int columnWidth = colWidths.get(c);
			sb.append(QueryResult.center(columnNames.get(c).substring(1), columnWidth, " ") + "|");
		}
		sb.append("\n");
		for(int c = 0; c < numCols; c++){
			int columnWidth = colWidths.get(c);
			sb.append(QueryResult.center("", columnWidth, "-") + "|");
		}
		sb.append("\n");
		for(List<Node> row:nodeRows){
			for(int c = 0; c < numCols; c++){
				int columnWidth = colWidths.get(c);
				sb.append(QueryResult.center(nodeVal(row.get(c)), columnWidth, " ") + "|");
			}
			sb.append("\n");
		}
		return sb.toString();
	}
	
	List<List<Node>> nodeRows;
	public void unalias(ShardedRedisTripleStore ts) {
		int numCols = columnNames.size();
		nodeRows = new ArrayList<List<Node>>();
		for(List<String> row : rows){
			List<Node> nodeRow = new ArrayList<Node>();
			for(int c = 0; c < numCols; c++){
				nodeRow.add(ts.getNodeFromAlias(row.get(c)));
				//row.set(c, ts.getStringFromAlias(row.get(c)));
			}
			nodeRows.add(nodeRow);
		}
	}

	
	/////////   ResultSet implementation //////////
	int itrPtr = 0;
	@Override
	public void remove() {
		// TODO Auto-generated method stub
		rows.remove(itrPtr);
		nodeRows.remove(itrPtr);
	}

	@Override
	public boolean hasNext() {
		return itrPtr < nodeRows.size();
	}

	
	List<Var> colVars;
	List<String> actualVarNames;
	Boolean iterInited = false;
	private void initIter(){
		colVars = new ArrayList<Var>();
		actualVarNames = new ArrayList<String>();
		for(String c : columnNames){
			if(!c.startsWith("META_")){
				colVars.add(Var.alloc(c.substring(1)));
				actualVarNames.add(c.substring(1));
			}
		}
		itrPtr = 0;
		iterInited = true;
	}
	@Override
	public QuerySolution next() {
		if(!iterInited) initIter();
		QuerySolutionMap result = new QuerySolutionMap();
		List<Node> nodeRow = nodeRows.get(itrPtr);
		
		Model m = ModelFactory.createDefaultModel();
		
		for(int c = 0; c < actualVarNames.size(); c++){
			result.add(actualVarNames.get(c), m.asRDFNode(nodeRow.get(c)));
		}
		
		itrPtr++;
		return result;
	}

	@Override
	public QuerySolution nextSolution() {
		return next();
	}

	@Override
	public Binding nextBinding() {
		if(!iterInited) initIter();
		BindingHashMap result = new BindingHashMap();
		List<Node> nodeRow = nodeRows.get(itrPtr);
		
		for(int c = 0; c < colVars.size(); c++){
			result.add(colVars.get(c), nodeRow.get(c));
		}
		
		itrPtr++;
		return result;
	}

	@Override
	public int getRowNumber() {
		if(!iterInited) initIter();
		return itrPtr;
	}

	@Override
	public List<String> getResultVars() {
		if(!iterInited) initIter();
		return actualVarNames;
	}

	@Override
	public Model getResourceModel() {
		return ModelFactory.createDefaultModel();
	}

}
