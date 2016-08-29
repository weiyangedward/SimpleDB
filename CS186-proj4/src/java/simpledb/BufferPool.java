package simpledb;


import java.io.*;
import java.security.Permission;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
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

    private int                                     numPages;
    private HashMap<PageId, Page>                   pages; // hashmap for <pageid, page> in pool
    private ArrayList<Page>                         lruList;
    // lru list to keep page sorted, the last one if the least recent used
    private HashMap<TransactionId, Boolean>         commited_tid;
    private HashMap<TransactionId, HashSet<PageId>> touched_page_by_tid;
    private LockManager                             lm;

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
        commited_tid = new HashMap<TransactionId, Boolean>();
        touched_page_by_tid = new HashMap<TransactionId, HashSet<PageId>>();
        lm = new LockManager(200);
    }

    /**
     * lockManager class
     * <p>
     * This class implements a strict two phase locking:
     * 1. if a read lock is held by t1, and t2 asks for a write lock, t2 will be blocked.
     * 2. all acquired locks by a transaction will be released at once in the end of transaction.
     * 3. if a write lock is held by t1, and t2 asks for a read lock, t2 will be blocked.
     * 3. if a read lock is held by t1, and t2 asks for a read lock, t2 will succeed.
     * <p>
     * Lock manager uses timeout to detect deadlock and throw exception, which requires use of wait and synchronized
     */
    public
    class LockManager
    {
        // read lock
        private HashMap<PageId, Integer>                read_lock_count;
        // count num of read locks for a page id
        private HashMap<PageId, HashSet<TransactionId>> read_lock_pageid_heldby_tid;
        // assign tid with same read lock to a page id

        // write lock
        private HashMap<PageId, TransactionId> write_lock_pageid_heldby_tid;
        private HashMap<PageId, ReentrantLock> write_lock_pid_lock;

        private final int timeout;

        /**
         * constructor
         */
        public
        LockManager(int timeout)
        {
            read_lock_count = new HashMap<PageId, Integer>();
            read_lock_pageid_heldby_tid = new HashMap<PageId, HashSet<TransactionId>>();

            write_lock_pageid_heldby_tid = new HashMap<PageId, TransactionId>();
            write_lock_pid_lock = new HashMap<PageId, ReentrantLock>();

            this.timeout = timeout;
        }

        /**
         * acquire write lock
         *
         * @param tid
         * @param pid
         */
        public synchronized
        void acquireWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException
        {
//            System.out.format("acquireWriteLock ...\n");
            if (read_lock_count.containsKey(pid))
            {
                System.out.format("tid %s, read_lock_count has pid %s\n", tid, pid);
            }
            if (write_lock_pageid_heldby_tid.containsKey(pid))
            {
                System.out.format(
                        "tid %s, write_lock_pageid_heldby_tid has pid %s, tid  %s\n", tid, pid,
                        write_lock_pageid_heldby_tid.get(pid)
                                 );

            }
            if (read_lock_count.containsKey(pid) &&
                (
                        write_lock_pageid_heldby_tid.containsKey(pid) &&
                        write_lock_pageid_heldby_tid.get(pid) != tid
                ))
            {
                // promote read lock to write lock if tid is the only one that reads a pid
                while (read_lock_count.get(pid) > 1 || (
                        read_lock_count.get(pid) == 1 &&
                        !read_lock_pageid_heldby_tid.get(pid).contains(tid)
                ))
                {
                    long startTime = System.currentTimeMillis();
                    try
                    {
                        wait(timeout); // wait until while condition becomes false, synchronized is needed
                    }
                    catch (Exception e)
                    {
                        System.out.println(e);
                    }

                    if (System.currentTimeMillis() - startTime >= timeout)
                    {

                        throw new TransactionAbortedException(); // throw exception if deadlock
                    }
                }
            }

            if (!write_lock_pid_lock.containsKey(pid))
            { write_lock_pid_lock.put(pid, new ReentrantLock()); }

            long startTime = System.currentTimeMillis();
            System.out.format("tid %s, waiting for write-lock pid %s, startTime %d\n", tid, pid, startTime);
            try
            {
                // lock when no one holds the lock, or try with timeout

                if (write_lock_pid_lock.get(pid).tryLock() || write_lock_pid_lock.get(pid).tryLock(timeout, TimeUnit.MILLISECONDS))
                {
                    System.out.format("tid %s, get write lock at %s\n", tid, System.currentTimeMillis());
                }
            }
            catch (Exception e)
            {
                System.out.println("Except: acquireWriteLock " + e);
            }

            if (System.currentTimeMillis() - startTime >= timeout)
            {
                System.out.format(
                        "Except: tid %s, waiting for write-lock pid %s, endTime %d\n", tid, pid,
                        System.currentTimeMillis()
                                 );
                throw new TransactionAbortedException(); // throw exception if deadlock
            }

            if (!write_lock_pageid_heldby_tid.containsKey(pid))
            { write_lock_pageid_heldby_tid.put(pid, tid); }
            else
            { write_lock_pageid_heldby_tid.put(pid, tid); }
        }

        /**
         * release write lock
         *
         * @param tid
         * @param pid
         */
        public synchronized
        void releaseWriteLock(TransactionId tid, PageId pid)
        {
            System.out.format("tid %s, releaseWriteLock on pid %s\n", tid, pid);
            write_lock_pid_lock.get(pid).unlock();
            write_lock_pageid_heldby_tid.remove(pid);
        }

        /**
         * acquire read lock
         *
         * @param tid
         * @param pid
         */
        public synchronized
        void acquireReadLock(TransactionId tid, PageId pid) throws TransactionAbortedException
        {
//            System.out.format("acquireReadLock ...\n");
            if (write_lock_pid_lock.containsKey(pid))
            {
                while (write_lock_pid_lock.get(pid).isLocked() && write_lock_pageid_heldby_tid.get(pid) != tid)
                {
                    long startTime = System.currentTimeMillis();
                    System.out.format("tid %s, waiting for read-lock %s, startTime %d\n", tid, pid, startTime);
                    try
                    {
                        wait(timeout);
                    }
                    catch (Exception e)
                    {
                        System.out.println(e);
                    }

                    if (System.currentTimeMillis() - startTime >= timeout)
                    {
                        System.out.format(
                                "Except: tid %s, waiting to read-lock %s, endTime %d\n", tid, pid,
                                System.currentTimeMillis()
                                         );
                        throw new TransactionAbortedException();
                    }
                }
            }

            if (!read_lock_count.containsKey(pid))
            { read_lock_count.put(pid, 1); }
            else
            { read_lock_count.put(pid, read_lock_count.get(pid) + 1); }

            if (!read_lock_pageid_heldby_tid.containsKey(pid))
            { read_lock_pageid_heldby_tid.put(pid, new HashSet<TransactionId>()); }
            read_lock_pageid_heldby_tid.get(pid).add(tid);
        }

        /**
         * release read lock
         *
         * @param tid
         * @param pid
         */
        public synchronized
        void releaseReadLock(TransactionId tid, PageId pid)
        {
//            System.out.format("releaseReadLock ...\n");
            if (!read_lock_count.containsKey(pid))
            { return; }

            if (read_lock_count.get(pid) >= 1)
            { read_lock_count.put(pid, read_lock_count.get(pid) - 1); }

            read_lock_pageid_heldby_tid.get(pid).remove(tid);
        }

        /**
         * release locks on a page
         *
         * @param tid
         * @param pid
         */
        public synchronized
        void releaseLockOnAPage(TransactionId tid, PageId pid)
        {
//            System.out.format("releaseLockOnAPage ...\n");
            releaseWriteLock(tid, pid);
            releaseReadLock(tid, pid);
        }

        /**
         * release all locks acquired by a tid
         *
         * @param tid
         */
        public synchronized
        void releaseAllLocks(TransactionId tid)
        {
            System.out.format("tid %s, releaseAllLocks\n", tid);
            // release write locks
            Iterator<Map.Entry<PageId, TransactionId>> iter = write_lock_pageid_heldby_tid.entrySet().iterator(); // use iterator to remove keys while iterating
            while (iter.hasNext())
            {
                Map.Entry<PageId, TransactionId> entry = iter.next();
                if (entry.getValue() == tid)
                {
                    write_lock_pid_lock.get(entry.getKey()).unlock();
                    write_lock_pid_lock.remove(entry.getKey()); // has to remove Reentrantlock here, since the lock was created by another obj, and so even after unlock, it still won't be able to hold by another obj in a different thread
                    iter.remove();
                }
            }

            // release read locks
            for (PageId pid : read_lock_pageid_heldby_tid.keySet())
            {
                if (read_lock_pageid_heldby_tid.get(pid).contains(tid))
                {
                    if (read_lock_count.get(pid) >= 1)
                    { read_lock_count.put(pid, read_lock_count.get(pid) - 1); }
                    read_lock_pageid_heldby_tid.get(pid).remove(tid);
                }
            }

            System.out.format(
                    "tid %s, write_lock_pageid_heldby_tid size %d\n", tid, write_lock_pageid_heldby_tid.size());
            for (PageId pid : write_lock_pageid_heldby_tid.keySet())
            {
                System.out.format("tid %s, left pid: %s, tid %s\n", tid, pid, write_lock_pageid_heldby_tid.get(pid));
            }

            for (PageId pid : read_lock_pageid_heldby_tid.keySet())
            {
                System.out.format(
                        "tid %s, read_lock_count size %d\n", tid, read_lock_count.get(pid));
                System.out.format(
                        "tid %s, read_lock_pageid_heldby_tid size %d\n", tid,
                        read_lock_pageid_heldby_tid.get(pid).size()
                                 );
            }
        }

        /**
         * return true if a tid has write or read lock on a page
         *
         * @param tid
         * @param pid
         * @return
         */
        public synchronized
        boolean isLockHeld(TransactionId tid, PageId pid)
        {
            if (write_lock_pageid_heldby_tid.get(pid) == tid || read_lock_pageid_heldby_tid.get(pid).contains(tid))
            { return true; }
            return false;
        }

    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.
     * <p>
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
        System.out.format("tid %s, BP getPage, pid %s, perm %s\n", tid, pid, perm);
//        System.out.format("TransactionId: %s, PageId %s, Permissions %s\n", tid, pid, perm);
        try
        {
            if (perm == Permissions.READ_WRITE)
            {
                lm.acquireWriteLock(tid, pid);
            }
            else if (perm == Permissions.READ_ONLY)
            {
                lm.acquireReadLock(tid, pid);
            }
        }
        catch (TransactionAbortedException e)
        {
            System.out.format("Except: tid %s, try to lock pid %s, perm %s\n", tid, pid, perm);
            throw new TransactionAbortedException();
        }
        catch (Exception e)
        {
            System.out.println(e);
            throw new DbException("other exception");
        }

//        synchronized (this)
//        {
        System.out.format("tid %s, BP gooooootPage, pid %s, perm %s\n", tid, pid, perm);
        Page pageFound = pages.get(pid);
        if (pageFound == null)
        {
            // evict a page from pool if full
            if (isFull())
            { evictPage(); }

            try
            {
                //                System.out.format("BufferPool finding page %d table id: %d\n", pid.pageNumber()
                // , pid.getTableId());
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


        if (!touched_page_by_tid.containsKey(tid))
        { touched_page_by_tid.put(tid, new HashSet<PageId>()); }
        touched_page_by_tid.get(tid).add(pid);

        return pageFound;
//        }
    }

    /**
     * return true if pool is full
     *
     * @return
     */
    private
    boolean isFull()
    {
        return lruList.size() == numPages;
    }

    /**
     * Releases the lock on a page.
     * <p>
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
        lm.releaseLockOnAPage(tid, pid);
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
        transactionComplete(tid, true);
    }

    /**
     * FORCE: force all pages to disk once transaction is complete
     * <p>
     * Commit or abort a given transaction; release all locks associated
     * to the transaction.
     * <p>
     * 1. if commit is true, then flush dirty pages to disk.
     * 2. if commit is false, then abort, and revert changes made by
     * tid, by restoring page to its on-disk state via reading pages
     * from disk.
     * <p>
     * Assume system does not crash during transactionComplete(),
     * therefore no recovery is needed. FORCE and NO-STEAL ensures
     * that no un-do or re-do is needed.
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
        System.out.format("tid %s, transactionComplete ...\n", tid);
        commited_tid.put(tid, commit);

        // 1) tid has commited, write all dirty pages to disk
        if (commit == true)
        {
//            System.out.format("commit tid: %s\n", tid);
            flushPages(tid);
        }
        // 2 )tid has not commited (aborted),
        // replace dirty pages with clean pages from disk
        else
        {
            if (touched_page_by_tid.containsKey(tid))
            {
                for (PageId pid : touched_page_by_tid.get(tid))
                {
                    discardPage(pid);
                    try
                    {
                        pages.put(pid, pages.get(pid).getBeforeImage());
                    }
                    catch (Exception e)
                    {
                        System.out.println(e);
                    }
                }
            }
        }
        lm.releaseAllLocks(tid);
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
        pages.remove(pid);
    }

    /**
     * Return true if the specified transaction has a lock on the
     * specified page
     */
    public
    boolean holdsLock(TransactionId tid, PageId pid)
    {
        // some code goes here
        // not necessary for proj1
        if (lm.isLockHeld(tid, pid))
        { return true; }
        return false;
    }


    /**
     * Add a tuple to the specified table behalf of transaction tid.
     * <p>
     * Will acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     * <p>
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
        System.out.format("BP insertTuple %s\n", t);
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        table.insertTuple(tid, t);

        // getPage() with READ_WRITE permission
        HeapPage page = (HeapPage) getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.markDirty(true, tid);

        if (!touched_page_by_tid.containsKey(tid))
        { touched_page_by_tid.put(tid, new HashSet<PageId>()); }
        touched_page_by_tid.get(tid).add(page.getId());
//        System.out.format("tid: %s, pid in hashmap: %s\n", tid, touched_page_by_tid.get(tid));
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * <p>
     * Will acquire a write lock on the page the tuple is removed from.
     * May block if the lock cannot be acquired.
     * <p>
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
        HeapPage page = (HeapPage) getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);

        if (!touched_page_by_tid.containsKey(tid))
        { touched_page_by_tid.put(tid, new HashSet<PageId>()); }
        touched_page_by_tid.get(tid).add(page.getId());
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
            { flushPage(page.getId()); }
        }
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
//        System.out.format("flushPage for pid ...\n");
        DbFile table = Database.getCatalog().getDbFile(pid.getTableId());
        Page   page  = pages.get(pid);
        if (page == null)
        { return; }
        else
        {
            table.writePage(page);
//            System.out.format("write page: %s\n", page);
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
        System.out.format("tid %s, flushPage\n", tid);
        if (touched_page_by_tid.containsKey(tid))
        {
            for (PageId pid : touched_page_by_tid.get(tid))
            {
                flushPage(pid);
            }
            touched_page_by_tid.remove(tid);
        }
    }

    /**
     * NO-STEAL: no evict dirty pages to disk.
     * <p>
     * Discards a page from the buffer pool.
     * <p>
     * Dirty pages are written to disk only after its transaction is commited.
     * <p>
     * A non-dirty pages gets written to disk immediately.
     * <p>
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * <p>
     * Use LRU cache eviction policy.
     * <p>
     * The LRU cache is a hash table of keys and double linked nodes.
     * The hash table makes the time of get() to be O(1).
     * The list of double linked nodes make the nodes adding/removal operations O(1).
     */
    private synchronized
    void evictPage() throws DbException
    {
        // some code goes here
        // not necessary for proj1
        for (int i = lruList.size() - 1; i >= 0; i--)
        {
            Page          curr_page = lruList.get(i);
            TransactionId tid       = curr_page.isDirty();
            // evict 1) clean page; or 2) committed dirty pages to disk
            if (tid == null || (tid != null &&
                                commited_tid.containsKey(tid) &&
                                commited_tid.get(tid) == true
                ))
            {
                lruList.remove(i);
                PageId pid = curr_page.getId();
                try
                {
                    flushPage(pid);
                }
                catch (Exception e)
                {
                    System.out.println(e);
                }
                pages.remove(pid);
                return;
            }
        }
        throw new DbException("all " + lruList.size() + " pages are dirty and are not commited\n");
    }

}
