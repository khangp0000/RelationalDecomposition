package uwdb.discovery.dependency.approximate.common.sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.h2.value.Value;

public class AttributeSet implements IAttributeSet {
	protected int numAttributes;
	protected BitSet set;
	int currIterator;

	public static long cloneTime = 0;

	public static void resetConeTime() {
		cloneTime = 0;
	}

	public double toDouble() {
		double retVal = 0;
		for (int i = 0; i < length(); i++) {
			retVal += (set.get(i)) ? Math.pow(2, i) : 0;
		}
		return retVal;
	}

	public AttributeSet(int numAttributes) {
		this.numAttributes = numAttributes;
		this.set = new BitSet(numAttributes);
		currIterator = 0;
	}

	public IAttributeSet clone() {
		long startTime = System.currentTimeMillis();
		AttributeSet other = new AttributeSet(this);
		cloneTime += (System.currentTimeMillis() - startTime);
		return other;
	}

	public int length() {
		return numAttributes;
	}

	public int cardinality() {
		return set.cardinality();
	}

	public boolean isEmpty() {
		return set.isEmpty();
	}

	public void add(int index) {
		set.set(index);
	}

	public void flip(int index) {
		set.flip(index);
	}

	public void remove(int index) {
		set.clear(index);
	}

	public void removeItemIdx(int index) {
		int k = 1;
		int i;
		for (i = set.nextSetBit(0); i >= 0 & k++ < index; i = set.nextSetBit(i + 1))
			;
		set.clear(i);
	}

	public boolean contains(int index) {
		return set.get(index);
	}

	public void add(int fromIndex, int toIndex) {
		set.set(fromIndex, toIndex);
	}

	public void flip(int fromIndex, int toIndex) {
		set.flip(fromIndex, toIndex);
	}

	public void remove(int fromIndex, int toIndex) {
		set.clear(fromIndex, toIndex);
	}

	public IAttributeSet complement() {
		AttributeSet value = (AttributeSet) clone();
		value.flip(0, numAttributes);
		return value;
	}

	public IAttributeSet union(IAttributeSet other) {
		AttributeSet otherSet = (AttributeSet) other;
		AttributeSet value = (AttributeSet) clone();
		value.set.or(otherSet.set);
		return value;
	}

	public IAttributeSet minus(IAttributeSet other) {
		AttributeSet otherSet = (AttributeSet) other;
		AttributeSet intersectionSet = (AttributeSet) intersect(otherSet);
		AttributeSet complementSet = (AttributeSet) intersectionSet.complement();
		return intersect(complementSet);
	}

	public IAttributeSet intersect(IAttributeSet other) {
		AttributeSet otherSet = (AttributeSet) other;
		AttributeSet value = (AttributeSet) clone();
		value.set.and(otherSet.set);
		return value;
	}

	public void intersectNonConst(IAttributeSet other) {
		AttributeSet otherSet = (AttributeSet) other;
		this.set.and(otherSet.set);
	}

	public boolean intersects(IAttributeSet other) {
		AttributeSet otherSet = (AttributeSet) other;
		return this.set.intersects(otherSet.set);
	}

	public boolean contains(IAttributeSet other) {
		for (int i = other.nextAttribute(0); i >= 0; i = other.nextAttribute(i + 1)) {
			if (!contains(i)) {
				return false;
			}
		}
		return true;
	}

	public int nextAttribute(int afterIndex) {
		return set.nextSetBit(afterIndex);
	}

	public int nextUnSetAttribute(int afterIndex) {
		return set.nextClearBit(afterIndex);
	}

	@Override
	public int hashCode() {
//    	byte[] setByteArr = set.toByteArray();
//    	int retVal = Arrays.hashCode(setByteArr); 
//    	return   retVal;
		return set.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
//        AttributeSet other = (AttributeSet) obj;
//        if(other.cardinality() != this.cardinality())
//        	return false;
//        
//        for (int i = nextAttribute(0), j = other.nextAttribute(0);
//             ((i != -1) || (j != -1));
//             j = other.nextAttribute(i+1), i = nextAttribute(i+1)) {
//            if(i != j)
//            {
//                return false;
//            }
//        }
//        return true;

		if (obj instanceof AttributeSet) {
			AttributeSet o = (AttributeSet) obj;
			return o.numAttributes == o.numAttributes && this.set.equals(o.set);
		}
		return false;
	}

	@Override
	public String toString() {
		return set.toString();
	}

	@Override
	public boolean hasNext() {
		int nextSetBit = set.nextSetBit(currIterator);
		return nextSetBit >= 0;
	}

	@Override
	public Integer next() {
		int nextSetBit = set.nextSetBit(currIterator);
		currIterator = nextSetBit + 1;
		return nextSetBit;
	}

	public void resetIterator() {
		currIterator = 0;
	}

	@Override
	public void or(IAttributeSet other) {
		AttributeSet otherSet = (AttributeSet) other;
		this.set.or(otherSet.set);

	}

	public AttributeSet(int[] setlist, int numAttributes) {
		this(numAttributes);
		for (int i : setlist) {
			this.set.set(i);
		}
	}
	
	public AttributeSet(AttributeSet o) {
		this.currIterator = 0;
		this.numAttributes = o.numAttributes;
		this.set = new BitSet();
		this.set = (BitSet) o.set.clone();
	}
	
	public List<Integer> setIdxList() {
		List<Integer> ret = new ArrayList<>();
		for (int i = set.nextSetBit(0); i >= 0 && i < numAttributes; i = set.nextSetBit(i+1)) {
			ret.add(i);
		}
		return ret;
	}

	public String bitString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < numAttributes; ++i) {
			sb.append(set.get(i) ? 1 : 0);
		}
		return sb.toString();
	}
}
