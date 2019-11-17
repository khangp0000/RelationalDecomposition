package uwdb.discovery.dependency.approximate.common;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

public class Transversals implements Iterator<IAttributeSet> {

	private Set<IAttributeSet> hyperedges;
	private Set<IAttributeSet> generatedTransversals;
	private Set<IAttributeSet> returnedTransversals;
	
	private static IAttributeSet emptyAS=null;
		
	public Transversals(Collection<IAttributeSet> hyperedges, int numAtts) {
		this.hyperedges = new HashSet<IAttributeSet>();
		returnedTransversals = new HashSet<IAttributeSet>();
		generatedTransversals =   new HashSet<IAttributeSet>();
		generatedTransversals.add(getEmptySet(numAtts));
		for(IAttributeSet edge: hyperedges) {
			addHyperedge(edge);
		}
	}
	
	
	
	public Transversals(int numAtts) {
		this.hyperedges = new HashSet<IAttributeSet>();
		returnedTransversals = new HashSet<IAttributeSet>();
		generatedTransversals =   new HashSet<IAttributeSet>();
		generatedTransversals.add(getEmptySet(numAtts));
	}
	
	private static IAttributeSet getEmptySet(int numAttributes) {
		if(emptyAS == null || emptyAS.length()!=numAttributes )
			emptyAS = new AttributeSet(numAttributes);
		
		return emptyAS;
	}
	
	public void addHyperedge(IAttributeSet edge) {
		if(hyperedges.contains(edge))
			return;	
		hyperedges.add(edge);
		
		//all the transversals that no longer cover the new edge
		Set<IAttributeSet> transversalsToRemove = new HashSet<IAttributeSet>();
		for(IAttributeSet transversal: generatedTransversals) {
			if(!transversal.intersects(edge)) {
				transversalsToRemove.add(transversal);
			}
		}
		for(IAttributeSet transversal: returnedTransversals) {
			if(!transversal.intersects(edge)) {
				transversalsToRemove.add(transversal);
			}
		}
		//we do not want to return these
		generatedTransversals.removeAll(transversalsToRemove);
		//we will never attempt to return these, so there is no point in saving them
		returnedTransversals.removeAll(transversalsToRemove);
		
		
		for(IAttributeSet transversal: transversalsToRemove) {
			for(int i=edge.nextAttribute(0); i >=0 ; i = edge.nextAttribute(i+1)) {
				IAttributeSet newTransversal = transversal.clone();
				newTransversal.add(i);
				if(isMinimal(newTransversal))
					generatedTransversals.add(newTransversal);
			}
		}
		
	}
	
	private boolean isMinimal(IAttributeSet tr) {
		for(IAttributeSet curr: generatedTransversals) {
			if(tr.contains(curr))
				return false;
		}
		for(IAttributeSet curr: returnedTransversals) {
			if(tr.contains(curr))
				return false;
		}
		return true;
	}
	@Override
	public boolean hasNext() {
		return (!generatedTransversals.isEmpty());
	}
	@Override
	public IAttributeSet next() {
		Iterator<IAttributeSet> it = generatedTransversals.iterator();
		IAttributeSet removed = it.next();
		it.remove();
		returnedTransversals.add(removed);
		return removed;
	}
	
	public static void main(String[] args) {
		IAttributeSet s1 = new AttributeSet(6);
		IAttributeSet s2 = new AttributeSet(6);
		IAttributeSet s3 = new AttributeSet(6);
		IAttributeSet s4 = new AttributeSet(6);
		IAttributeSet s5 = new AttributeSet(6);
		IAttributeSet s6 = new AttributeSet(6);
		
		s1.add(1); s1.add(3);
		s2.add(0); s2.add(2);s2.add(4); s2.add(5);
		s3.add(4);
		s4.add(2); s4.add(3);
		s5.add(0); s5.add(4);
		s6.add(0); s6.add(1); s6.add(2); s6.add(3);
		
		HashSet<IAttributeSet> edges = new HashSet<IAttributeSet>();
		edges.add(s1); edges.add(s2); 
		edges.add(s3); edges.add(s4); edges.add(s5); edges.add(s6);
		
		Transversals tr = new Transversals(edges,6);
		while(tr.hasNext()) {
			System.out.println(tr.next());
		}
		
		IAttributeSet s7 = new AttributeSet(6);
		s7.add(0); s7.add(5);
		tr.addHyperedge(s7);
		while(tr.hasNext()) {
			System.out.println(tr.next());
		}
	}
}
