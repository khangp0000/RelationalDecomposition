package uwdb.discovery.dependency.approximate.common.sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import uwdb.discovery.dependency.approximate.common.dependency.DataDependency;
import uwdb.discovery.dependency.approximate.common.dependency.JoinDependency;

public class JDSetCommonLHS {
	
	private IAttributeSet lhs;
	private HashSet<JoinDependency> lhsJDs;
	private int size;
	
	public JDSetCommonLHS(IAttributeSet lhs)
    {
        this.size = 0;
        this.lhs = lhs;
        this.lhsJDs = new HashSet<JoinDependency>();        
    }

	public Iterator<JoinDependency> getJDIt(){
		return lhsJDs.iterator();
	}
	
    //accessor functions
    public long size() {
        return size;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public boolean contains(JoinDependency dependency)
    {
    	if(!dependency.getlhs().equals(dependency.getlhs()))
    		return false;
    	return lhsJDs.contains(dependency);
    }

    public void add(JoinDependency dependency)
    {
    	if(!dependency.getlhs().equals(lhs))
    		return;
        if(!contains(dependency)) {
        	lhsJDs.add(dependency);
        }
    }

    public void remove(JoinDependency dependency) {
    	if(!dependency.getlhs().equals(lhs))
    		return;
        size--;
        lhsJDs.remove(dependency);
    }

    
}
