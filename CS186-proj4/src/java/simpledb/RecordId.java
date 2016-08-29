package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to
 * a specific tuple
 * on a specific page
 * of a specific table.
 */
public
class RecordId implements Serializable
{

    private static final long serialVersionUID = 1L;

    private PageId page_id; // page id
    private int tuple_no; // tuple number

    /**
     * Creates a new RecordId referring to the specified
     * PageId and tuple number.
     *
     * @param pid     the pageid of the page on which the
     *                tuple resides
     * @param tupleno the tuple number within the page.
     */
    public
    RecordId(PageId pid, int tupleno)
    {
        // some code goes here
        tuple_no = tupleno;
        page_id = pid;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public
    int tupleno()
    {
        // some code goes here
        return tuple_no;
    }

    /**
     * @return the page id this RecordId references.
     */
    public
    PageId getPageId()
    {
        // some code goes here
        return page_id;
    }

    /**
     * Two RecordId objects are considered equal if they
     * represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public
    boolean equals(Object o)
    {
        // some code goes here
        //        throw new UnsupportedOperationException("implement this");
        if (o == this) return true;
        if (o == null) return false;
        if (o.getClass() != this.getClass()) return false;
        RecordId that = (RecordId) o;
        if (that.tupleno() == this.tupleno() && that.getPageId().equals(this.getPageId())) return true;
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public
    int hashCode()
    {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        int hash = 17;
        hash = 31*hash + tuple_no;
        hash = 31*hash + page_id.hashCode();
        return hash;
    }

}
