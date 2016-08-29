package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 *
 * This creates a statistics for a table, and will be used to estimate cost of
 * query plans.
 *
 * e.g.:
 * filter: f = const
 * selectivity (fraction of tuples in result) = (height(ntups_in_bucket) / width(range_of_values)) / ntups_in_table
 *
 * tuples_in_result = selectivity x ntups_in_table
 */
public
class IntHistogram
{
    private int num_buckets;
    private int[] buckets;
    private int min;
    private int max;
    private int range;
    private int total_added_tuples;
    private int width;
    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the
     * "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you
     * shouldn't simply store every value that you see in a sorted list.
     *
     * Each value in a bucket i is >= i and < i+1
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for
     *                histogramming
     * @param max     The maximum integer value that will ever be passed to this class for
     *                histogramming
     */
    public
    IntHistogram(int buckets, int min, int max)
    {
        // some code goes here
        this.total_added_tuples = 0;
        this.num_buckets = buckets;
        this.min = min;
        this.max = max;
        this.range = this.max - this.min + 1;
        
        if (this.num_buckets > this.range)
            this.num_buckets = this.range;
        this.buckets = new int[this.num_buckets]; // init all buckets to 0
        this.width = (int)Math.floor((this.range) / this.num_buckets);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public
    void addValue(int v)
    {
        // some code goes here
//        System.out.format("addValue %d\n", v);
        if (v < min || v > max)
            throw new IllegalArgumentException("input value out of bound");

        buckets[indexOfBucket(v)]++;
        total_added_tuples++;
    }

    /**
     * helper function to use binary search to find index of bucket given a value
     * @param key
     * @return
     */
    private int indexOfBucket(int v)
    {
        int index = ((v % (max+1)) - min) % num_buckets;
        return index;

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public
    double estimateSelectivity(Predicate.Op op, int v)
    {

        // some code goes here
        double selectivity = -1.0;
        double frac, part;
        if (v < min)
            part = 0;
        switch (op)
        {
            case EQUALS:
            case LIKE:
                selectivity = estimateEqualSelectivity(v);
                break;
            case GREATER_THAN:
                selectivity = estimateGreaterThanSelectivity(v);
//                System.out.format("GreaterThan: %g\n", estimateGreaterThanSelectivity(v));
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateEqualSelectivity(v) + estimateGreaterThanSelectivity(v);
//                System.out.format("Equal: %g, GreaterThan: %g\n", estimateEqualSelectivity(v) , estimateGreaterThanSelectivity(v));
                break;
            case LESS_THAN:
                selectivity = 1.0 - estimateEqualSelectivity(v) - estimateGreaterThanSelectivity(v);
//                System.out.format("Equal: %g, GreaterThan: %g\n", estimateEqualSelectivity(v) , estimateGreaterThanSelectivity(v));
                break;
            case LESS_THAN_OR_EQ:
                selectivity = 1.0 - estimateGreaterThanSelectivity(v);
                break;
            case NOT_EQUALS:
                selectivity = 1.0 - estimateEqualSelectivity(v);
                break;
        }
//        if (num_buckets != 101 && max > 0)
//        {
//            System.out.println(toString());
//            System.out.format("op(%s %d): %g\n", op, v, selectivity);
//        }
        return selectivity;
    }

    private double estimateEqualSelectivity(int v)
    {

        if (v < min || v > max)
            return 0.0;

//        System.out.format("estimateEqualSelectivity: v: %d, index: %d, h %d, width %d, total_added_tuples %d\n", v, indexOfBucket(v), buckets[indexOfBucket(v)], width, total_added_tuples );
        return ((double)buckets[indexOfBucket(v)] / width) / total_added_tuples;
    }

    /**
     * helper function to estimate GreaterThanSelectivity
     * @param v
     * @return
     */
    private double estimateGreaterThanSelectivity(int v)
    {
//        System.out.format("estimateGreaterThanSelectivity ...\n");
        if (v < min)
            return 1.0;

        if (v > max)
            return 0.0;

        double selectivity = 0.0;
        int indexOfV = indexOfBucket(v);
        // compute part of the bucket using floor boundary of bucket = (index + 1)*width - v
        selectivity += ((double)( (indexOfV+1)*width - indexOfV - 1) / width) * ((double)buckets[indexOfV] / total_added_tuples);

//        System.out.format("estimateGreaterThanSelectivity: v: %d, indexOfV: %d, hOfV: %d, hOfV+1: %d, total_added_tuples %d, selectivity %g\n", v, indexOfV, buckets[indexOfV], buckets[indexOfV+1], total_added_tuples, selectivity);

        for (int i=indexOfV+1; i<num_buckets; i++)
        {
            selectivity += (double)buckets[i] / total_added_tuples;
        }

        return selectivity;
    }

    /**
     * @return the average selectivity of this histogram.
     *
     * Compute the average over all buckets.
     *
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public
    double avgSelectivity()
    {
        // some code goes here
        double avg = 0.0;
        for (int i=0; i<num_buckets; i++)
            avg += (double)(buckets[i] * buckets[i]);
        avg /= (double)(total_added_tuples * total_added_tuples);
        return avg;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public
    String toString()
    {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<num_buckets; i++)
            sb.append(i + ":" + buckets[i] + " ");
        return sb.toString();
    }
}
