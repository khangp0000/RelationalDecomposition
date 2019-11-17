package uwdb.discovery.dependency.approximate.common.dependency;


import uwdb.discovery.dependency.approximate.common.*;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public abstract class DataDependency implements IDataDependency {
    public IAttributeSet lhs;
    public IAttributeSet rhs;
    public Measure measure;
    private double valueFromData;
    
    public void setMeasure(double val) {
    	valueFromData=val;
    }
    
    public double getMeasure() {
    	return valueFromData;
    }

    public DataDependency(IAttributeSet lhs, IAttributeSet rhs, double lowerBound, double upperBound)
    {
        this.lhs = lhs;
        this.rhs = rhs;
        this.measure = new Measure(lowerBound, upperBound);
        valueFromData =-1;
    }

    public void setLHS(IAttributeSet lhs) {
    	this.lhs = lhs;
    	valueFromData =-1;
    }
    
    public void setRHS(IAttributeSet rhs) {
    	this.rhs = rhs;   
    	valueFromData =-1;
    }
    
    
    public Status isExact() {
        return isApproximate(0.0);
    }
    
    public Status isApproximate(double alpha) {
        return measure.isLessThan(alpha);
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(lhs.hashCode());
        builder.append(rhs.hashCode());
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        DataDependency other = (DataDependency) obj;
        return lhs.equals(other.lhs) && rhs.equals(other.rhs);
    }
    
    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder(lhs.toString());
    	sb.append("->->");
    	sb.append(rhs.toString());    	
    	sb.append(": ");
    	sb.append(this.getMeasure());
        return sb.toString();
    }
}
