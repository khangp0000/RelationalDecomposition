package uwdb.discovery.dependency.approximate.common;

import java.util.BitSet;

import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

public class BitSetMatrixGraph {

	int numNodes;
	IAttributeSet rows[];
	public BitSetMatrixGraph(int numNodes) {
		this.numNodes=numNodes;
		rows = new IAttributeSet[numNodes];
		for(int i=0 ; i < numNodes ; i++) {
			rows[i] = new AttributeSet(numNodes);
		}	
	}
	
	public void addUndirectedEdge(int n1, int n2) {
		rows[n1].add(n2);
		rows[n2].add(n1);
	}
	
	public IAttributeSet getNbrs(int n) {
		return rows[n];
	}
	
	public void extendToMaxIndependentSet(IAttributeSet indSet) {		
		for(int i=0 ; i < numNodes ; i++) {
			if(!indSet.contains(i)) {
				if(!rows[i].intersects(indSet)) { //i is not a neighbor of any elements in maxIndSet
					indSet.add(i);
				}
			}
		}		
	}
	
	public void addNodeToMaxIndependentSet(IAttributeSet indSet, int newNode) {
		//first, remove all neighbors of newNode rfom BitSet
		IAttributeSet newNodeNbrs = rows[newNode];
		for(int j=newNodeNbrs.nextAttribute(0) ; j >=0 ; j = newNodeNbrs.nextAttribute(j+1)){
			indSet.remove(j);
		}
		indSet.add(newNode);
	}
	
	public void addNodeAndExtend(IAttributeSet indSet, int newNode) {
		addNodeToMaxIndependentSet(indSet, newNode);
		extendToMaxIndependentSet(indSet);
	}
	
	
}
