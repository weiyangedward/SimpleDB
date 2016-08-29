package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about
 * base tables in a query.
 *
 * This class is not needed in implementing proj1 and proj2.
 */
public
class TableStats
{

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    static final int IOCOSTPERPAGE = 1000;

    private ArrayList<Object> field_histograms; // list of histograms, one for each field. Use Object so that it holds both of Int and String histogram
    private HeapFile table;
    private TupleDesc td;
    private int num_tuples;
    private int ioCostPerPage;

    /**
     * Constructor:
     * Create a new TableStats object, that keeps track of statistics on each
     * column (field) of a table. Use aggergate to compute the MIN and MAX value
     * of a field, which is needed to construct a histogram.
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public
    TableStats(int tableid, int ioCostPerPage)
    {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

//        System.out.format("my table id: %d\n", tableid);
        this.table = (HeapFile)Database.getCatalog().getDbFile(tableid);
        this.td = table.getTupleDesc();
        this.ioCostPerPage = ioCostPerPage;
        this.field_histograms = new ArrayList<Object>();
        int num_fields = this.td.numFields();

        // scan and get tuples from disk
        SeqScan ss = new SeqScan(null, tableid, "TableStats");
        try
        {
            ss.open();
            while (ss.hasNext())
            {
                ss.next();
                this.num_tuples++;
            }
            ss.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        /**
         * for each field, build a histogram
         * use aggregator to get MIN and MAX value of integer field
         */

        for (int i=0; i<num_fields; i++)
        {
            // perform aggregate from int field
            if (this.td.getFieldType(i) == Type.INT_TYPE)
            {
                // min aggregate over a field without groupby
                Aggregate min_agg = new  Aggregate(ss, i, -1, Aggregator.Op.MIN);                            int min = 0;
                try
                {
                    min_agg.open();
                    if (min_agg.hasNext())
                    {
                        Tuple t = min_agg.next();
                        min = ((IntField)t.getField(0)).getValue();
                    }
                    min_agg.close();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }

                // max aggregate over a field without groupby
                Aggregate max_agg = new  Aggregate(ss, i, -1, Aggregator.Op.MAX);
                int max = 0;
                try
                {
                    max_agg.open();
                    if (max_agg.hasNext())
                    {
                        Tuple t = max_agg.next();
                        max = ((IntField)t.getField(0)).getValue();
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }

                IntHistogram hist = new IntHistogram(NUM_HIST_BINS, min, max);

                try
                {
                    ss.open();
                    while (ss.hasNext())
                    {
                        Tuple t = ss.next();
                        hist.addValue(((IntField)t.getField(i)).getValue());
                    }
                    ss.close();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }

                this.field_histograms.add(hist);
            }
            // string field
            else
            {
                StringHistogram hist = new StringHistogram(NUM_HIST_BINS);
                try
                {
                    ss.open();
                    while (ss.hasNext())
                    {
                        Tuple t = ss.next();
                        hist.addValue(((StringField)t.getField(i)).getValue());
                    }
                    ss.close();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                this.field_histograms.add(hist);
            }
        }
    }

    /**
     * return TableStats obj given a table name
     * @param tablename
     * @return
     */
    public static
    TableStats getTableStats(String tablename)
    {
        return statsMap.get(tablename);
    }

    /**
     * update TableStats obj for a table
     * @param tablename
     * @param stats
     */
    public static
    void setTableStats(String tablename, TableStats stats)
    {
        statsMap.put(tablename, stats);
    }

    public static
    void setStatsMap(HashMap<String, TableStats> s)
    {
        try
        {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        }
        catch (NoSuchFieldException e)
        {
            e.printStackTrace();
        }
        catch (SecurityException e)
        {
            e.printStackTrace();
        }
        catch (IllegalArgumentException e)
        {
            e.printStackTrace();
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
        }

    }

    /**
     * return the hashmap of key=tablename, value=TableStats
     * @return
     */
    public static
    Map<String, TableStats> getStatsMap()
    {
        return statsMap;
    }

    /**
     * init a TableStats obj for all tables
     */
    public static
    void computeStatistics()
    {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext())
        {
            int        tableid = tableIt.next();
            TableStats s       = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }


    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public
    double estimateScanCost()
    {
        // some code goes here
        int num_page = table.numPages();
        return (double)num_page * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public
    int estimateTableCardinality(double selectivityFactor)
    {
        // some code goes here
        return (int)(num_tuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public
    double avgSelectivity(int field, Predicate.Op op)
    {
        // some code goes here
        double avg_selectivity = 0.0;
        // int field
        if (td.getFieldType(field) == Type.INT_TYPE)
        {
            IntHistogram hist = (IntHistogram)field_histograms.get(field);
            avg_selectivity = hist.avgSelectivity();
        }
        // string field
        else
        {
            StringHistogram hist = (StringHistogram)field_histograms.get(field);
            avg_selectivity = hist.avgSelectivity();
        }
        return avg_selectivity;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * Selectivity is the fraction of tuples in a table left after an operation.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public
    double estimateSelectivity(int field, Predicate.Op op, Field constant)
    {
        // some code goes here
        double selectivity = 0.0;
        // int field
        if (td.getFieldType(field) == Type.INT_TYPE)
        {
//            System.out.format("INT_TYPE, value: %d\n", ((IntField)constant).getValue());
            IntHistogram hist = (IntHistogram)field_histograms.get(field);
            selectivity = hist.estimateSelectivity(op, ((IntField)constant).getValue());
        }
        // string field
        else
        {
            StringHistogram hist = (StringHistogram)field_histograms.get(field);
            selectivity = hist.estimateSelectivity(op, ((StringField)constant).getValue());
        }

        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     */
    public
    int totalTuples()
    {
        // some code goes here
        return num_tuples;
    }

}
