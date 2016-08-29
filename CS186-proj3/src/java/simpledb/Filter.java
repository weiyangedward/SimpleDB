package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public
class Filter extends Operator
{

    private static final long serialVersionUID = 1L;

    private Predicate p;
    private DbIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public
    Filter(Predicate p, DbIterator child)
    {
        // some code goes here
        this.p = p;
        this.child = child;
    }

    public
    Predicate getPredicate()
    {
        // some code goes here
        return p;
    }

    public
    TupleDesc getTupleDesc()
    {
        // some code goes here
        return child.getTupleDesc();
    }

    public
    void open() throws DbException, NoSuchElementException,
                       TransactionAbortedException
    {
        // some code goes here
        super.open();
        child.open();
    }

    public
    void close()
    {
        // some code goes here
        super.close();
        child.close();
    }

    public
    void rewind() throws DbException, TransactionAbortedException
    {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * Optimization:
     * use hashmap for <field, list<tuple>> and iterator over tuples in list
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected
    Tuple fetchNext() throws NoSuchElementException,
                             TransactionAbortedException, DbException
    {
        // some code goes here
        Tuple next_tuple = null;
        Tuple tmp_tuple = null;
        while (child.hasNext())
        {
            tmp_tuple = child.next();
//            System.out.format("tmp_tuple = %s\n", tmp_tuple.toString());
            if (p.filter(tmp_tuple)) // if pass predicate, then replace next_tuple=null by tmp_tuple
            {
                next_tuple = tmp_tuple;
                break;
            }
        }
//        System.out.format("final next_tuple = %s\n", next_tuple.toString());
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
        child = children[0]; // there is only one child for filter
    }

}
