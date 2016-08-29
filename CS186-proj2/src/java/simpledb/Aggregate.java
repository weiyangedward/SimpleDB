package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min).
 *
 * Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public
class Aggregate extends Operator
{

    private static final long serialVersionUID = 1L;

    // private field
    private DbIterator child;
    private int gfield;
    private int afield;
    private Aggregator.Op op;
    private Aggregator agg;
    private boolean groupby = false;
    private DbIterator iterator;
    private TupleDesc td;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public
    Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop)
    {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.op = aop;
        Type gtype = null;
        Type atype = child.getTupleDesc().getFieldType(afield);
        if (gfield != Aggregator.NO_GROUPING)
        {
            this.groupby = true;
            gtype = child.getTupleDesc().getFieldType(gfield);
            Type[]   groupby_typeAr = {gtype, atype};
            String[] groupby_nameAr = {groupFieldName(), "null"};
            td = new TupleDesc(groupby_typeAr, groupby_nameAr);
        }
        else
        {
            Type[]   nogroupby_typeAr = {atype};
            String[] nogroupby_nameAr = {"null"};
            td = new TupleDesc(nogroupby_typeAr, nogroupby_nameAr);
        }

        /**
         * create aggregator matches type of afield
         */
        if (atype == Type.INT_TYPE)
            this.agg = new IntegerAggregator(gfield, gtype, afield, op);
        else
        {
//            System.out.format("aggregate type: STRING\n");
            this.agg = new IntegerAggregator(gfield, gtype, afield, op);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public
    int groupField()
    {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return
     * null;
     */
    public
    String groupFieldName()
    {
        // some code goes here
        String groupby_name = null;
        if (groupby)
            groupby_name = child.getTupleDesc().getFieldName(gfield);
        return groupby_name;
    }

    /**
     * @return the aggregate field
     */
    public
    int aggregateField()
    {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public
    String aggregateFieldName()
    {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public
    Aggregator.Op aggregateOp()
    {
        // some code goes here
        return op;
    }

    public static
    String nameOfAggregatorOp(Aggregator.Op aop)
    {
        return aop.toString();
    }

    public
    void open() throws NoSuchElementException, DbException, TransactionAbortedException
    {
        // some code goes here
//        System.out.format("aggregate open ...\n");
        super.open();
        child.open();
        try
        {
            while (child.hasNext())
            {
                this.agg.mergeTupleIntoGroup(child.next());
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        this.iterator = agg.iterator();
        iterator.open();
    }

    /**
     * Returns the next tuple.
     *
     * 1. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate.
     *
     * 2. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected
    Tuple fetchNext() throws TransactionAbortedException, DbException
    {
        // some code goes here
        Tuple next_tuple = null;
        if (iterator.hasNext())
            next_tuple = iterator.next();
        return next_tuple;
    }

    public
    void rewind() throws DbException, TransactionAbortedException
    {
        // some code goes here
        iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     *
     * 1. If there is no group by field,
     * this will have one field - the aggregate column.
     *
     * 2. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public
    TupleDesc getTupleDesc()
    {
        // some code goes here
        return td;
    }

    public
    void close()
    {
        // some code goes here
        super.close();
        child.close();
        iterator.close();
    }

    public
    boolean hasNext() throws DbException, TransactionAbortedException
    {
        return iterator.hasNext();
    }

    public
    Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
    {
        Tuple next_tuple = iterator.next();
//        System.out.format("next tuple: %s\n", next_tuple);
        return next_tuple;
    }

    @Override
    public
    DbIterator[] getChildren()
    {
        // some code goes here
        DbIterator[] children = {child};
        return children;
    }

    @Override
    public
    void setChildren(DbIterator[] children)
    {
        // some code goes here
        child = children[0];
    }

}
