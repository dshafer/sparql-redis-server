package translate.redis;

import java.util.ArrayList;
import java.util.List;

import main.ShardedRedisTripleStore;

import org.json.JSONArray;

public class QueryResult {
    
	List<String> columnNames;
	List<List<String>> rows;
	Boolean initialized;
	
	public QueryResult(){
		initialized = false;
		rows = new ArrayList<List<String>>();
	}
	private ArrayList<String> JSONArrayToArrayList(JSONArray ja){
		ArrayList<String> result = new ArrayList<String>(ja.length());
		for(int i = 0; i < ja.length(); i++){
			result.add(ja.getString(i));
		}
		return result;
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
        if (s == null || size <= s.length())
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
    
	public String asTable(){
		StringBuilder sb = new StringBuilder();
		int numCols = columnNames.size();
		List<Integer> colWidths = new ArrayList<Integer>(numCols);
		for(int c = 0; c < numCols; c++){
			colWidths.add(8);
		}
		for(List<String> row:rows){
			for(int c = 0; c < numCols; c++){
				if (colWidths.get(c) < row.get(c).length()){
					colWidths.set(c, row.get(c).length());
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
		for(List<String> row:rows){
			for(int c = 0; c < numCols; c++){
				int columnWidth = colWidths.get(c);
				sb.append(QueryResult.center(row.get(c), columnWidth, " ") + "|");
			}
			sb.append("\n");
		}
		return sb.toString();
	}
	public void unalias(ShardedRedisTripleStore ts) {
		int numCols = columnNames.size();
		for(List<String> row : rows){
			for(int c = 0; c < numCols; c++){
				row.set(c, ts.getStringFromAlias(row.get(c)));
			}
		}
	}

}
