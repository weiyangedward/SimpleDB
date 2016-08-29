package simpledb;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public
class TupleDesc implements Serializable
{
    private int numFields; // num of fields of a schema
    private TDItem[] schema; // fieldtypes and fieldnames of a schema

    /**
     * A help class to facilitate organizing
     * the information of each field
     */
    public static
    class TDItem implements Serializable
    {

        private static final long serialVersionUID = 1L;

        Type fieldType; // fieldtypes
        String fieldName; //  fieldnames

        // constructor
        public
        TDItem(Type t, String n)
        {
            this.fieldType = t;
            this.fieldName = n;
        }

        public
        String toString()
        {
            return fieldName + "(" + fieldType + ")";
        }

        // length of a field
        public int getLen()
        {
            return fieldType.getLen();
        }
    }

    /**
     * @return An iterator
     *      which iterates over all the field TDItems
     *      that are included in this TupleDesc
     */
    public
    Iterator<TDItem> iterator()
    {
        // some code goes here
        return new TDIterator();
    }

    /**
     * iterator class
     */
    private
    class TDIterator implements Iterator<TDItem>
    {
        private int i = 0;

        public boolean hasNext()
        {
            return i < schema.length;
        }

        public void remove()
        {
            throw new UnsupportedOperationException("remove in iterator is not allowed");
        }

        public TDItem next()
        {
            if (!hasNext()) throw new NoSuchElementException("schema underflow");
            TDItem item = schema[i];
            i++;
            return item;
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * constructor
     *
     * Create a new TupleDesc with typeAr.length fields with
     * fields of the specified types, with associated named fields.
     *
     * @param typeAr
     *      array specifying the number of and types of
     *      fields in this TupleDesc. It must contain
     *      at least one entry.
     *
     * @param fieldAr
     *      array specifying the names of the fields.
     *      Note that names may be null.
     */
    public
    TupleDesc(Type[] typeAr, String[] fieldAr)
    {
        // some code goes here
        schema = new TDItem[typeAr.length];
        for (int i=0; i<typeAr.length; i++)
            schema[i] = new TDItem(typeAr[i],fieldAr[i]);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length
     * fields with fields of the specified types, with anonymous
     * (unnamed) fields.
     *
     * @param typeAr
     *      array specifying the number of and types of fields in
     *      this TupleDesc. It must contain at least one entry.
     */
    public
    TupleDesc(Type[] typeAr)
    {
        // some code goes here
        schema = new TDItem[typeAr.length];
        for (int i=0; i<typeAr.length; i++)
            schema[i] = new TDItem(typeAr[i],null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public
    int numFields()
    {
        // some code goes here
        return schema.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field
     * of this TupleDesc.
     *
     * @param i
     *      index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public
    String getFieldName(int i) throws NoSuchElementException
    {
        // some code goes here
        if (i < 0 || i >= schema.length) throw new NoSuchElementException(i + " index is not valid");
        return schema[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *      The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public
    Type getFieldType(int i) throws NoSuchElementException
    {
        // some code goes here
        if (i < 0 || i >= schema.length) throw new NoSuchElementException(i + " index is not valid");
        return schema[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *      name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public
    int fieldNameToIndex(String name) throws NoSuchElementException
    {
        // some code goes here
        if (name == null) throw new NoSuchElementException("name is null");
        for (int i=0; i<schema.length; i++)
            if (name.equals(schema[i].fieldName))
                return i;
        throw new NoSuchElementException("no field has a matching name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public
    int getSize()
    {
        // some code goes here
        int size = 0;
        for (int i=0; i<schema.length; i++)
            size += schema[i].getLen();
        return size;
    }

    /**
     * Merge two TupleDescs into one,
     * with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1
     * and the remaining from td2.
     *
     * @param td1
     *      The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *      The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static
    TupleDesc merge(TupleDesc td1, TupleDesc td2)
    {
        // some code goes here
        int length = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[length];
        String[] fieldAr = new String[length];

        int i;
        for (i=0; i<td1.numFields(); i++)
        {
            typeAr[i] = td1.getFieldType(i);
            fieldAr[i] = td1.getFieldName(i);
        }
        for (int j=0; j<td2.numFields(); j++, i++)
        {
            typeAr[i] = td2.getFieldType(j);
            fieldAr[i] = td2.getFieldName(j);
        }

        TupleDesc newTD = new TupleDesc(typeAr, fieldAr);
        return newTD;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *      the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public
    boolean equals(Object o)
    {
        // some code goes here
        if (this == o) return true;
        if (o == null) return false;
        if (o.getClass() != this.getClass()) return false;
        TupleDesc that = (TupleDesc)o;
        if (this.numFields() != that.numFields()) return false;
        for (int i=0; i<schema.length; i++)
        {
            if (schema[i].fieldType != that.getFieldType(i) || schema[i].fieldName != that.getFieldName(i))
                return false;
        }
        return true;
    }

    public
    int hashCode()
    {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public
    String toString()
    {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<schema.length; i++)
            sb.append(schema[0].toString() + ",");
        return sb.toString();
    }
}
