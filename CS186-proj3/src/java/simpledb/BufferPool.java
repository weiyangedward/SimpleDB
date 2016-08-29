package simpledb;

import java.io.*;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 *
 * The BufferPool is also responsible for locking;
 * when a transaction fetches a page, BufferPool checks that the
 * transaction has the appropriate locks to read/write the page.
 */
public
class BufferPool
{
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int                   numPages;
    private HashMap<PageId, Page> pages; // hashmap for <pageid, page> in pool
    private ArrayList<Page> lruList; // lru list to keep page sorted, the last one if the least recent used

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public
    BufferPool(int numPages)
    {
        // some code goes here
        this.numPages = numPages;
        pages = new HashMap<PageId, Page>(numPages);
        lruList = new ArrayList<Page>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     *
     * The retrieved page should be looked up in the buffer pool.
     *
     * 1. If it is present, it should be returned.
     * 2. If it is not present, it should be added to the buffer pool and
     * returned.
     * 3. If there is insufficient space in the buffer pool, an page
     * should be evicted and the new page should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public
    Page getPage(TransactionId tid, PageId pid, Permissions perm)
    throws TransactionAbortedException, DbException
    {
        // some code goes here
        Page pageFound = pages.get(pid);
        if (pageFound == null)
        {
            // evict a page from pool if full
            if (isFull())
                evictPage();

            try
            {
//                System.out.format("BufferPool finding page %d table id: %d\n", pid.pageNumber(), pid.getTableId());
                pageFound = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
                pages.put(pid, pageFound);
                lruList.add(0, pageFound);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        // if page found
        // move found page to the head of lru list
        lruList.remove(pageFound);
        lruList.add(0, pageFound);
        return pageFound;
    }

    /**
     * return true if pool is full
     * @return
     */
    private boolean isFull()
    {
        return lruList.size() == numPages;
    }

    /**
     * Releases the lock on a page.
     *
     * Calling this is very risky, and may result in wrong behavior.
     * Think hard about who needs to call this and why, and why they can
     * run the risk of calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public
    void releasePage(TransactionId tid, PageId pid)
    {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public
    void transactionComplete(TransactionId tid) throws IOException
    {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Return true if the specified transaction has a lock on the
     * specified page
     */
    public
    boolean holdsLock(TransactionId tid, PageId p)
    {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated
     * to the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public
    void transactionComplete(TransactionId tid, boolean commit)
    throws IOException
    {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.
     *
     * Will acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by
     * calling their markDirty bit, and updates cached versions of any
     * pages that have been dirtied so that future requests see
     * up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public
    void insertTuple(TransactionId tid, int tableId, Tuple t)
    throws DbException, IOException, TransactionAbortedException
    {
        // some code goes here
        // not necessary for proj1
        HeapFile table = (HeapFile)Database.getCatalog().getDbFile(tableId);
        table.insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     *
     * Will acquire a write lock on the page the tuple is removed from.
     * May block if the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by
     * calling
     * their markDirty bit.  Does not need to update cached versions of
     * any pages that have
     * been dirtied, as it is not possible that a new page was created
     * during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t   the tuple to add
     */
    public
    void deleteTuple(TransactionId tid, Tuple t)
    throws DbException, TransactionAbortedException
    {
        // some code goes here
        // not necessary for proj1
        HeapPage page = (HeapPage) getPage(tid, t.getRecordId().getPageId(), null);
        page.deleteTuple(t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk
     * so will break simpledb if running in NO STEAL mode.
     */
    public synchronized
    void flushAllPages() throws IOException
    {
        // some code goes here
        // not necessary for proj1
        for (Page page : lruList)
        {
            if (page.isDirty() != null)
                flushPage(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized
    void discardPage(PageId pid)
    {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized
    void flushPage(PageId pid) throws IOException
    {
        // some code goes here
        // not necessary for proj1
        DbFile table = Database.getCatalog().getDbFile(pid.getTableId());
        Page page = pages.get(pid);
        if (page == null)
            return;
        else
        {
            table.writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized
    void flushPages(TransactionId tid) throws IOException
    {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     *
     * Use LRU cache eviction policy.
     *
     * The LRU cache is a hash table of keys and double linked nodes.
     * The hash table makes the time of get() to be O(1).
     * The list of double linked nodes make the nodes adding/removal operations O(1).
     */
    private synchronized
    void evictPage() throws DbException
    {
        // some code goes here
        // not necessary for proj1
        Page last_page = lruList.remove(lruList.size()-1);
        PageId pageid = last_page.getId();
        pages.remove(pageid);
    }

}
