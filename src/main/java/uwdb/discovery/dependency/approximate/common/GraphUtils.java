package uwdb.discovery.dependency.approximate.common;

import java.util.*;


public class GraphUtils {
	private final int n;
	private int componentCount;
	private int[] components;
	private boolean solved;
	private boolean[] visited;
	private List<List<Integer>> graph;

	/**
	 * @param graph - An undirected graph as an adjacency list.
	*/
	public GraphUtils(List<List<Integer>> graph) {
		if (graph == null) throw new NullPointerException();
		this.n = graph.size();
		this.graph = graph;
	}

	public int[] getComponents() {
		solve();
		return components;
	}

	public void solve() {
	    if (solved) return;

	    visited = new boolean[n];
	    components = new int[n];
	    for (int i = 0; i < n; i++) {
	      if (!visited[i]) {
	        componentCount++;
	        dfs(i);
	      }
	    }

	    solved = true;
	  }

	private void dfs(int at) {
	    visited[at] = true;
	    components[at] = componentCount;
	    for (int to : graph.get(at))
	      if (!visited[to])
	        dfs(to);
	  }
	
	

	public static List<List<Integer>> createGraph(int n) {
		List<List<Integer>> graph = new ArrayList<>(n);
		for (int i = 0; i < n; i++) graph.add(new ArrayList<>(n/2));
		return graph;
	}

	public static void addUndirectedEdge(List<List<Integer>> graph, int from, int to) {
		graph.get(from).add(to);
	    graph.get(to).add(from);
	 }

}
