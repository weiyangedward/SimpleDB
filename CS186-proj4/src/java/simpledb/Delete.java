package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator
 * and removes them from the table they belong to.
 */
public
class Delete extends Operator
{

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator    child;
    private Tuple         count_tuple;
    private boolean valid = true;
    private int count_deleted_tuples;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public
    Delete(TransactionId t, DbIterator child)
    {
        // some code goes here
        System.out.format("tid %s, Delete \n", t);
        this.tid = t;
        this.child = child;

        Type[]   typeAr  = {Type.INT_TYPE};
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
        count_deleted_tuples = 0;
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
        count_deleted_tuples = 0;
        valid = true;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
                System.out.format("tid %s, Delete tuple via BP: %s\n", tid, t);
                Database.getBufferPool().deleteTuple(tid, t); // delete tuple from table in disk
                count_deleted_tuples++;
            }
            catch (Exception e)
            {
                System.out.format("Except: Delete fetchNext " + e);
                throw new TransactionAbortedException();
            }
        }

        if (valid)
        {
            count_tuple.setField(0, new IntField(count_deleted_tuples));
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
