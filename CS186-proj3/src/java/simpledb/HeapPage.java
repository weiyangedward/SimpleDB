package simpledb;

import javax.naming.OperationNotSupportedException;
import java.util.*;
import java.io.*;
import java.lang.Math;

/**
 * Each instance of HeapPage stores data
 * for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public
class HeapPage implements Page
{

    HeapPageId pid; // page id
    TupleDesc  td; // tuple desc
    byte       header[]; // header of tuples in page
    Tuple      tuples[]; // tuples in page
    int        numSlots; // num of tuples in page
    boolean dirty;
    TransactionId tid;

    byte[] oldData;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to:
     *
     * floor((BufferPool.PAGE_SIZE*8) / (tuple size * 8 + 1))
     *
     * where tuple size is the size of tuples in this
     * database table, which can be determined via
     * {@link Catalog#getTupleDesc}.
     *
     * The number of 8-bit header words is equal to:
     *
     * ceiling(no. tuple slots / 8)
     *
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#PAGE_SIZE
     */
    public
    HeapPage(HeapPageId id, byte[] data) throws IOException
    {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
//        System.out.format("numSlots = %d\n", numSlots);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
//        System.out.format("header length = %d\n", header.length);
        for (int i = 0; i < header.length; i++)
        { header[i] = dis.readByte(); }

        try
        {
            // allocate and read the actual records of this page
            tuples = new Tuple[numSlots];
            for (int i = 0; i < tuples.length; i++)
            {
                /**
                 * readNextTuple() only returns used tuple
                 * so only front of tuples[] is used
                 * the rest is all empty
                 */
                tuples[i] = readNextTuple(dis, i);
            }
        }
        catch (NoSuchElementException e)
        {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();

        this.dirty = false;
        this.tid = null;
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private
    int getNumTuples()
    {
        // some code goes here
        int num_tuples = (int)Math.floor( (BufferPool.PAGE_SIZE*8) / (td.getSize()*8+1) );
        return num_tuples;

    }

    /**
     * Computes the number of bytes in the header of a page in a
     * HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile
     * with each tuple occupying tupleSize bytes
     */
    private
    int getHeaderSize()
    {
        // some code goes here
        int num_header = (int)Math.ceil(numSlots/8.0);
        return num_header;

    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public
    HeapPage getBeforeImage()
    {
        try
        {
            return new HeapPage(pid, oldData);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public
    void setBeforeImage()
    {
        oldData = getPageData().clone();
    }

    /**
     * @return the PageId associated with this page.
     */
    public
    HeapPageId getId()
    {
        // some code goes here
        //        throw new UnsupportedOperationException("implement this");
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private
    Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException
    {
        // if associated bit is not set,
        // read forward to the next tuple,
        // and return null.
        if (!isSlotUsed(slotId))
        {
            for (int i = 0; i < td.getSize(); i++)
            {
                try
                {
                    dis.readByte();
                }
                catch (IOException e)
                {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple    t   = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try
        {
            for (int j = 0; j < td.numFields(); j++)
            {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        }
        catch (java.text.ParseException e)
        {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     *
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * This is very important, as it enables randomAccess to pages
     * in the disk sequentially!! Try to understand the code.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public
    byte[] getPageData()
    {
        int                   len  = BufferPool.PAGE_SIZE;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream      dos  = new DataOutputStream(baos);

        // create the header of the page
        for (int i = 0; i < header.length; i++)
        {
            try
            {
                dos.writeByte(header[i]);
            }
            catch (IOException e)
            {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++)
        {

            // empty slot
            if (!isSlotUsed(i))
            {
                for (int j = 0; j < td.getSize(); j++)
                {
                    try
                    {
                        dos.writeByte(0);
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++)
            {
                Field f = tuples[i].getField(j);
                try
                {
                    f.serialize(dos);

                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int    zerolen = BufferPool.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes  = new byte[zerolen];
        try
        {
            dos.write(zeroes, 0, zerolen);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        try
        {
            dos.flush();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static
    byte[] createEmptyPageData()
    {
        int len = BufferPool.PAGE_SIZE;
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *          already empty.
     */
    public
    void deleteTuple(Tuple t) throws DbException
    {
        // some code goes here
        // not necessary for lab1
        for (int i=0; i<tuples.length; i++)
        {
            if (tuples[i] != null && this.tuples[i].equals(t))
            {
//                System.out.format("found tuple: %d\n", i);
                tuples[i] = null;
                markDirty(true, null);
                markSlotUsed(i, false);
                t.setRecordId(null);
                return; // tuple found return, so not throw exception
            }
        }

        throw new DbException("tuple not in page.");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public
    void insertTuple(Tuple t) throws DbException
    {
        // some code goes here
        // not necessary for lab1
        if (getNumEmptySlots() == 0 || !this.td.equals(t.getTupleDesc()))
            throw new DbException("no empty slot or tuple desc not match.");

        for (int i=0; i<tuples.length; i++)
        {
            if (!isSlotUsed(i))
            {
                t.setRecordId(new RecordId(pid, i));
                tuples[i] = t;
                markSlotUsed(i, true);
                markDirty(true, tid);
                break;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     *
     * if not dirty, then tid should set to null
     */
    public
    void markDirty(boolean dirty, TransactionId tid)
    {
        // some code goes here
        // not necessary for lab1
        this.dirty = dirty;
        if (dirty)
            this.tid = tid;
        else
            this.tid = null;
    }

    /**
     * Returns
     * 1. the tid of the transaction that last dirtied this page,
     * 2. or null if the page is not dirty
     */
    public
    TransactionId isDirty()
    {
        // some code goes here
        // Not necessary for lab1
        return tid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public
    int getNumEmptySlots()
    {
        // some code goes here
        int empty_slots = 0;
        for (int i=0; i<numSlots; i++)
        {
            if (!isSlotUsed(i))
                empty_slots ++;
        }
        return empty_slots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     *
     * Java is using big-endian, so bits in each byte is count from right to left
     */
    public
    boolean isSlotUsed(int i)
    {
        // some code goes here
        try
        {
            int  byte_index  = i / 8;
            /**
             * 0..7 is the 1st byte,
             * so 8 is the 1st bit in the 2nd byte,
             * and has bit_index = 0
               */
            int  bit_index   = i % 8;
//            System.out.format("bit_index = %d\n", bit_index);
            byte inside_byte = header[byte_index];
            if ((inside_byte & 1<<bit_index) > 0) return true;
        }
        catch(Exception e)
        {
            System.out.format("index = %d\n", i);
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private
    void markSlotUsed(int i, boolean value)
    {
        // some code goes here
        // not necessary for lab1
        int  byte_index  = i / 8;
        int  bit_index   = i % 8;

        if (value)
        {
            // replace '0' with '1' in the tuple position
            header[byte_index] |= ( (byte)1 << bit_index );
        }
        else
        {
            // replace '1' with '0' in the tuple position
            header[byte_index] &= ~( (byte)1 << bit_index);
        }
    }

    /**
     * @return an iterator over all tuples on this page
     * (calling remove on this iterator throws an UnsupportedOperationException)
     *
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public
    Iterator<Tuple> iterator()
    {
        // some code goes here
        return new TupleIterator();
    }

    private class TupleIterator implements Iterator<Tuple>
    {
        private int i = 0;

        public boolean hasNext()
        {
//            System.out.format("hasNext i = %d\n", i);
            return (i < tuples.length && isSlotUsed(i));
        }

        public Tuple next()
        {
            if (!hasNext()) throw new NoSuchElementException("tuple underflow");
            return tuples[i++];
        }

        public void remove()
        {
            throw new UnsupportedOperationException("remove not allowed");
        }
    }

}

