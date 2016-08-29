package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a
 * collection of tuples in no particular order.
 *
 * Tuples are stored on pages, each
 * of which is a fixed size, and the file is simply a collection
 * of those pages.
 *
 * HeapFile works closely with HeapPage. The format of HeapPages is
 * described in the HeapPage constructor.
 *
 * HeapFile reads pages from disk.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public
class HeapFile implements DbFile
{
    private File      file; // the corresponding file path
    private TupleDesc td; // tuple description
    private int       maxPageNo; // the upper limit of page number in a heapfile

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public
    HeapFile(File f, TupleDesc td)
    {
        // some code goes here
        this.file = f;
        this.td = td;
        this.maxPageNo = (int) f.length() / BufferPool.PAGE_SIZE - 1;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public
    File getFile()
    {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public
    int getId()
    {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public
    TupleDesc getTupleDesc()
    {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return td;
    }

    /**
     * Read the specified page from disk
     *
     * RandomAccessFile is used to provide efficient access of the file
     *
     * @param pid
     * @return
     * @throws IOException
     */
    public
    Page readPage(PageId pid) throws IOException
    {
        // some code goes here
        int    page_no = pid.pageNumber();
        int    offset  = BufferPool.PAGE_SIZE * page_no;
        byte[] data    = new byte[BufferPool.PAGE_SIZE]; // buffer to store a file
        /**
         * RandomAccessFile allows to read from any part of a file
         * so that we can start reading from the offset
         */
        RandomAccessFile raf = null;

        /**
         * read file
         */
        try
        {
            raf = new RandomAccessFile(file, "r");
            // move the file pointer to the offset
            raf.seek((long) offset);
            raf.read(data, 0, BufferPool.PAGE_SIZE);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            raf.close();
        }

        /**
         * return pages read
         */
        return new HeapPage((HeapPageId) pid, data);
    }

    /**
     * Push the specified page to disk.
     *
     * @param p The page to write.  page.getId().pageno()
     *          specifies
     *          the offset into the file where the page should be
     *          written.
     */
    public
    void writePage(Page page) throws IOException
    {
        // some code goes here
        // not necessary for proj1
        int page_no = page.getId().pageNumber();
        int offset = page_no * BufferPool.PAGE_SIZE;
        byte[] data = page.getPageData();
        RandomAccessFile raf = null;

        try
        {
            raf = new RandomAccessFile(file, "rw");
            raf.seek((long)offset);
            raf.write(data, 0, BufferPool.PAGE_SIZE);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            raf.close();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public
    int numPages()
    {
        // some code goes here
        int num_page = (int) file.length() / BufferPool.PAGE_SIZE;
        return num_page;
    }

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to add.  This tuple should be
     *            updated to reflect that
     *            it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     */
    public
    ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
    throws DbException, IOException, TransactionAbortedException
    {
        // some code goes here
        // not necessary for proj1
        for (int i=0; i<numPages(); i++)
        {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), null);
            try
            {
                page.insertTuple(t);
                ArrayList<Page> modified_page = new ArrayList<Page>();
                modified_page.add(page);
                return modified_page;
            }
            catch (Exception e)
            {
//                e.printStackTrace();
//                System.out.format("HeapFile.insertTuple(): page is full.\n");
            }
        }

        /**
         * all pages are full, need a new page
         */
        byte[] data = HeapPage.createEmptyPageData();
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), data);
        writePage(newPage);
        return insertTuple(tid, t); // recursively insert to new page
    }

    /**
     * Removes the specifed tuple from the file on behalf of the
     * specified transaction.
     * This method will acquire a lock on the affected pages of
     * the file, and may block until the lock can be acquired.
     */
    public
    Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException
    {
        // some code goes here
        // not necessary for proj1
        HeapPage current_page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null);
        current_page.deleteTuple(t);
        return current_page;
    }

    /**
     * Returns an iterator over all the tuples stored in this
     * DbFile (all pages in HeapFile).
     *
     * The iterator must use BufferPool.getPage() to access page in HeapFile.
     *
     * BufferPool.getPage() will:
     * 1. first check if a page is in memory
     * 2. if not, then use HeapFile.readPage() to fetch page from disk
     *
     * @param tid
     * @return
     */
    public
    DbFileIterator iterator(TransactionId tid)
    {
        // some code goes here
        return new FileIterator(tid);
    }

    /**
     * tuple iterator of pages in a heapfile that
     * implements DbFileIterator
     */
    private
    class FileIterator implements DbFileIterator
    {
        private int             currentPageNo = 0; // init current page no to be the 1st page
        private Page            currentPage   = null;
        private PageId          currentPageId = null;
        private Iterator<Tuple> tuples        = null; // tuples in a page
        private TransactionId   tid           = null;
        private int             tableId       = 0;

        public
        FileIterator(TransactionId tid)
        {
            this.tid = tid;
            tableId = getId(); // heapfile id
        }

        /**
         * open iterator
         * load all tuples of the 1st page of a table
         * do not load the enrire table into memory using open()
         *
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public
        void open() throws DbException, TransactionAbortedException
        {
            loadPage();
        }

        public
        boolean hasNext() throws DbException, TransactionAbortedException
        {
            /**
             * if all tuples in current page is null, then stop
             * otherwise keep going
             */
            if (tuples != null)
            {
                if (tuples.hasNext())
                {
                    return true; // use HeapPage iterator to iterate through tuples
                }
                /**
                 * if tuples is empty, then try to fetch next page
                 */
                else
                {
                    if (currentPageNo < maxPageNo)
                    {
                        currentPageNo++;
                        loadPage();
                        return hasNext(); // recursively find the next tuple
                    }
                }
            }
            return false;
        }

        public
        Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
        {
            if (tuples != null)
            { return tuples.next(); }
            else
            { throw new NoSuchElementException("tuple underflow"); }
        }

        /**
         * reset the iterator to the start
         *
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public
        void rewind() throws DbException, TransactionAbortedException
        {
            currentPageNo = 0;
            loadPage();
        }

        /**
         * close the iterator
         */
        public
        void close()
        {
            currentPageNo = 0;
            tableId = 0;
            currentPageId = null;
            currentPage = null;
            tuples = null;
        }

        /**
         * helper function to load a page given currentPageNo
         */
        private
        void loadPage() throws DbException
        {
            try
            {
                currentPageId = new HeapPageId(
                        tableId, currentPageNo); // init current page to be the 1st page of a heapfile (table)
                currentPage = Database.getBufferPool().getPage(tid, currentPageId, null); // fetch current page
                tuples = ((HeapPage) currentPage)
                        .iterator(); // HeapPage iterator to iterate through all tuples in a page
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

}

