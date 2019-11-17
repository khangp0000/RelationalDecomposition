package uwdb.discovery.dependency.approximate.common.sets;



import java.util.Iterator;

public interface IAttributeSet extends Iterator<Integer>{
    int length();
    int cardinality();
    boolean isEmpty();

    void add(int index);
    void flip(int index);
    void remove(int index);
    boolean contains(int index);

    void add(int fromIndex, int toIndex);
    void flip(int fromIndex, int toIndex);
    void remove(int fromIndex, int toIndex);    
    
    void or(IAttributeSet other);

    IAttributeSet clone();
    IAttributeSet complement();
    IAttributeSet union(IAttributeSet other);
    IAttributeSet minus(IAttributeSet other);
    IAttributeSet intersect(IAttributeSet other);
    public boolean intersects(IAttributeSet other);
    
    public void intersectNonConst(IAttributeSet other);
    
    boolean contains(IAttributeSet other);    
    public void removeItemIdx(int index);

    //afterIndex or beforeIndex are inclusive
    int nextAttribute(int afterIndex);
    
    void resetIterator();
}
