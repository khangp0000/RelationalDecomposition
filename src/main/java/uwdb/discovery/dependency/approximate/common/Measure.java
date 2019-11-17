package uwdb.discovery.dependency.approximate.common;


public class Measure
{
    protected static double error = 1E-6;
    protected double lowerBound;
    protected double upperBound;

    public Measure()
    {
        this(0.0, Double.MAX_VALUE);
    }

    public Measure(double lowerBound, double upperBound)
    {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public void updateLowerBound(double value)
    {
        this.lowerBound = Math.max(this.lowerBound, value);
    }

    public void updateUpperBound(double value)
    {
        this.upperBound = Math.min(this.upperBound, value);
    }

    public double getLowerBound()
    {
        return this.lowerBound;
    }

    public double getUpperBound()
    {
        return this.upperBound;
    }

    public void setValue(double value) throws Exception {
        boolean moreThanLowerBound = (value >= lowerBound) || (lowerBound - value) <= error;
        boolean lessThanUpperBound = (value <= upperBound) || (value - upperBound) <= error;

        value = Math.max(value, error);

        if(moreThanLowerBound && lessThanUpperBound)
        {
            //we are good, all our previous computations are correct!
            lowerBound = value;
            upperBound = value;
        }
        else
        {
            throw new Exception("Trying to set value not within the bounds!");
        }
    }

    public double getValue() throws Exception
    {
        if(isExactValue())
        {
            return lowerBound;
        }
        else
        {
            throw new Exception("Exact value not available");
        }
    }

    public boolean isExactValue()
    {
        return lowerBound == upperBound;
    }

    public Status isLessThan(double alpha)
    {
        if(upperBound <= alpha)
        {
            return Status.TRUE;
        }
        else if(lowerBound > alpha)
        {
            return Status.FALSE;
        }
        else
        {
            return Status.UNKNOWN;
        }
    }

    public Status isGreaterThan(double alpha)
    {
        if(lowerBound >= alpha)
        {
            return Status.TRUE;
        }
        else if(upperBound < alpha)
        {
            return Status.FALSE;
        }
        else
        {
            return Status.UNKNOWN;
        }
    }
}
