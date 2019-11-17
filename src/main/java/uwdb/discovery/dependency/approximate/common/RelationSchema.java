package uwdb.discovery.dependency.approximate.common;


import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

public class RelationSchema {
    protected int numAttributes;

    public RelationSchema(int numAttributes) {
        this.numAttributes = numAttributes;
    }
    public int getNumAttributes()
    {
        return numAttributes;
    }

    public IAttributeSet getEmptyAttributeSet()
    {
        return new AttributeSet(numAttributes);
    }
}
