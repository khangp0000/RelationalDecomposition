package uwdb.discovery.dependency.approximate.entropy;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import uwdb.discovery.dependency.approximate.common.sets.AttributeSet;
import uwdb.discovery.dependency.approximate.common.sets.IAttributeSet;

import java.util.BitSet;

public class Utilities {
    public static int getHashCode(long[] data)
    {
        HashCodeBuilder builder = new HashCodeBuilder();
        for(int i = 0; i < data.length; i++)
        {
            builder.append(data[i]);
        }
        return builder.toHashCode();
    }

    public static int getHashCode(long[] data, IAttributeSet set) {
        HashCodeBuilder builder = new HashCodeBuilder();
        for(int i = set.nextAttribute(0); i >= 0; i = set.nextAttribute(i+1))
        {
            builder.append(data[i]);
        }
        return builder.toHashCode();
    }

    public static int getHashCode(String[] data, IAttributeSet set) {
        HashCodeBuilder builder = new HashCodeBuilder();
        for(int i = set.nextAttribute(0); i >= 0; i = set.nextAttribute(i+1))
        {
            builder.append(data[i]);
        }
        return builder.toHashCode();
    }

    public static int getHashCode(BitSet set) {
        HashCodeBuilder builder = new HashCodeBuilder();
        appendBitSetHashToHashBuilder(builder, set);
        return builder.toHashCode();
    }


    public static void appendBitSetHashToHashBuilder(HashCodeBuilder builder, BitSet bitSet) {
        int current32BitsStartIndex = 0;
        int current32BitsValue = 0;
        for(int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i+1))
        {
            if((i - current32BitsStartIndex) > 31)
            {
                builder.append(current32BitsValue);
                current32BitsStartIndex += 32;
            }
            current32BitsValue += (int)Math.pow(2, i);
        }
        builder.append(current32BitsValue);
    }

    public static boolean equals(BitSet set1, BitSet set2)
    {
        if(set1.size() == set2.size())
        {
            for(int i = 0; i < set1.size(); i++)
            {
                if(set1.get(i) != set2.get(i))
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }

}
