package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc
 * object and contain Field objects with the data for each
 * field.
 */
public
class Tuple implements Serializable
{

    private static final long serialVersionUID = 1L;
    private TupleDesc td; // schema of a tuple
    private int N; // num of fields
    private RecordId record_id; // record id refer to a tuple
    private Field[] fieldValues; // fieldvalues

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *      the schema of this tuple. It must be a valid TupleDesc
     *      instance with at least one field.
     */
    public
    Tuple(TupleDesc td)
    {
        // some code goes here
        if (td == null || td.numFields() == 0)
            throw new NoSuchElementException("td is not valid");

        this.td = td;
        N = td.numFields();
        fieldValues = new Field[N];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public
    TupleDesc getTupleDesc()
    {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public
    RecordId getRecordId()
    {
        // some code goes here
        return record_id;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public
    void setRecordId(RecordId rid)
    {
        // some code goes here
        record_id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *      index of the field to change. It must be a valid index.
     * @param f
     *      new value for the field.
     */
    public
    void setField(int i, Field f) throws NoSuchElementException
    {
        // some code goes here
        if (i < 0 || i >= N) throw new NoSuchElementException(i + " index is not valid");
        fieldValues[i] = f;
    }

    /**
     * @param i
     *      field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public
    Field getField(int i)
    {
        // some code goes here
        if (i < 0 || i >= N) throw new NoSuchElementException(i + " index is not valid");
        return fieldValues[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format
     * needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public
    String toString()
    {
        // some code goes here
        int i;
        StringBuilder sb = new StringBuilder();
        for (i=0; i<N-1; i++)
            sb.append(fieldValues[i].toString() + "\t");

        sb.append(fieldValues[i].toString());
        return sb.toString();
//        throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public
    Iterator<Field> fields()
    {
        // some code goes here
        return new FieldIterator();
    }

    /**
     * iterator class of fieldValues
     */
    private class FieldIterator implements Iterator<Field>
    {
        private int i = 0;

        public boolean hasNext()
        {
            return i < N;
        }

        public Field next()
        {
            if (!hasNext()) throw new NoSuchElementException("field underflow");
//            Field tmp_field = fieldValues[i];
//            i++;
//            return tmp_field;
            return fieldValues[i++];
        }

        public void remove()
        {
            throw new UnsupportedOperationException("remove in iterator is not allowed");
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null) return false;
        if (this.getClass() != o.getClass()) return false;
        Tuple that = (Tuple) o;
        if (!this.getTupleDesc().equals(that.getTupleDesc())) return false;
        for (int i=0; i<fieldValues.length; i++)
        {
            if (!this.getField(i).equals(that.getField(i))) return false;
        }
        return true;
    }

    public int hashCode()
    {
        int hash = 17;
        for (int i=0; i<fieldValues.length; i++)
            hash = 31 * hash + getField(i).hashCode();
        return hash;
    }
}
