package simpledb;

/**
 * Unique identifier for HeapPage objects.
 */
public
class HeapPageId implements PageId
{
    private int table_id; // table id
    private int page_no; // page num

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public
    HeapPageId(int tableId, int pgNo)
    {
        // some code goes here
        table_id = tableId;
        page_no = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public
    int getTableId()
    {
        // some code goes here
        return table_id;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public
    int pageNumber()
    {
        // some code goes here
        return page_no;
    }

    /**
     * @return a hash code for this page, represented by the concatenation
     * of
     * the table number and the page number (needed if a PageId is used
     * as a
     * key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public
    int hashCode()
    {
        // some code goes here
        int hash = 17;
        hash = 31*hash + table_id;
        hash = 31*hash + page_no;
        return hash;
//        throw new UnsupportedOperationException("implement this");
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Integer temp_table_id = new Integer(this.table_id);
        sb.append(temp_table_id);
        sb.append(" ");
        Integer temp_page_no = new Integer(this.page_no);
        sb.append(temp_page_no);
        return sb.toString();
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    public
    boolean equals(Object o)
    {
        // some code goes here
        if (o == this) return true;
        if (o == null) return false;
        if (o.getClass() != this.getClass()) return false;
        HeapPageId that = (HeapPageId) o;
        if (that.pageNumber() == this.pageNumber() && that.getTableId() == this.getTableId()) return true;
        return false;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public
    int[] serialize()
    {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
