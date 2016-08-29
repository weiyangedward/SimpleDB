package simpledb;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public
class Insert extends Operator
{

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private Tuple count_tuple;
    private boolean valid = true;
    private int count_inserted_tuples;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public
    Insert(TransactionId t, DbIterator child, int tableid)
    throws DbException
    {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableid = tableid;

        Type[] typeAr = {Type.INT_TYPE};
        String[] fieldAr = {"null"}; // has to be 'null' to pass test
        this.count_tuple = new Tuple(new TupleDesc(typeAr, fieldAr));
    }

    public
    TupleDesc getTupleDesc()
    {
        // some code goes here
        return count_tuple.getTupleDesc();
    }

    public
    void open() throws DbException, TransactionAbortedException
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
        valid = true;
        count_inserted_tuples = 0;
    }

    /**
     * Inserts all tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records,
     *          or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected
    Tuple fetchNext() throws TransactionAbortedException, DbException
    {
        // some code goes here


        while (child.hasNext())
        {
            try
            {
                Tuple t = child.next();
                Database.getBufferPool().insertTuple(tid, tableid, t); // insert tuple to table in disk
                count_inserted_tuples++;
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        if (valid)
        {
            count_tuple.setField(0, new IntField(count_inserted_tuples));
            valid = false;
            return count_tuple;
        }
        else
        {
            return null;
        }
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
