package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access
 * method that reads each tuple of a table in no particular order (e.g., as
 * they are laid out on disk).
 *
 * This is an operator (iterator based) to access tuples.
 *
 * SeqScan is an operator that implements DbIterator interface.
 */
public
class SeqScan implements DbIterator
{

    private static final long serialVersionUID = 1L;

    private int table_id;
    private TransactionId tid;
    private String table_alias;
    private DbFileIterator tuple_iterator;
    private boolean opened;

    /**
     * Creates a sequential scan over the specified table as
     * a part of the specified transaction.
     *
     * @param tid        The transaction this scan is running
     *                   as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by
     *                   the parser); the returned
     *                   tupleDesc should have fields with
     *                   name tableAlias.fieldName
     *                   (note: this class is not responsible
     *                   for handling a case where
     *                   tableAlias or fieldName are null. It
     *                   shouldn't crash if they
     *                   are, but the resulting name can be
     *                   null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public
    SeqScan(TransactionId tid, int tableid, String tableAlias)
    {
        // some code goes here
        System.out.format("tid %s, SS, table id %s\n", tid, tableid);
        this.table_id = tableid;
        this.tid = tid;
        this.table_alias = tableAlias;
        // SS iterate tuple using HP's iterator, so HP's iterator should use READ_WRITE perm
        this.tuple_iterator = Database.getCatalog().getDbFile(table_id).iterator(tid); // get tuple iterator from heapfile

    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public
    String getTableName()
    {
        String table_name = Database.getCatalog().getTableName(table_id);
        return table_name;
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public
    String getAlias()
    {
        // some code goes here
        return table_alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public
    void reset(int tableid, String tableAlias)
    {
        // some code goes here
        table_id = tableid;
        table_alias = tableAlias;
    }

    /**
     * constructor with table alias = table name
     * @param tid
     * @param tableid
     */
    public
    SeqScan(TransactionId tid, int tableid)
    {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * start iterator
     * @throws DbException
     * @throws TransactionAbortedException
     */
    public
    void open() throws DbException, TransactionAbortedException
    {
        // some code goes here
        tuple_iterator.open();
        this.opened = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public
    TupleDesc getTupleDesc()
    {
        // some code goes here
        TupleDesc old_td = Database.getCatalog().getTupleDesc(table_id);
        String[] new_fieldNames = new String[old_td.numFields()];
        Type[] new_fieldTypes = new Type[old_td.numFields()];
        for (int i=0; i<old_td.numFields(); i++)
        {
            String old_fieldName = old_td.getFieldName(i);
            String new_fieldName = table_alias + "." + old_fieldName;
            new_fieldNames[i] = new_fieldName;
            new_fieldTypes[i] = old_td.getFieldType(i);
        }
        return new TupleDesc(new_fieldTypes, new_fieldNames);
    }

    public
    boolean hasNext() throws TransactionAbortedException, DbException
    {
        // some code goes here
        return tuple_iterator.hasNext();
    }

    public
    Tuple next() throws NoSuchElementException,
                        TransactionAbortedException, DbException
    {
        // some code goes here
        if (!hasNext())
        {
            throw new NoSuchElementException("no next tupel");
        }
        if (!opened)
        {
            throw new DbException("iterator is closed");
        }

        Tuple t = null;
        try
        {
            t = tuple_iterator.next();
            System.out.format("tid %s, SS read in tuple: %s\n", tid, t);
        }
        catch (Exception e)
        {
            System.out.println(e);
            throw new TransactionAbortedException();
        }
        return t;
    }

    /**
     * end iterator
     */
    public
    void close()
    {
        // some code goes here
        tuple_iterator.close();
        this.opened = false;
    }

    public
    void rewind() throws DbException, NoSuchElementException,
                         TransactionAbortedException
    {
        // some code goes here
        tuple_iterator.rewind();
    }
}
