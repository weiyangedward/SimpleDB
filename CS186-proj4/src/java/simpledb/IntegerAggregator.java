package simpledb;

import java.util.Map;
import java.util.ArrayList;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 *
 * This integer aggregator supports MIN, MAX, SUM, AVG, COUNT.
 */
public
class IntegerAggregator implements Aggregator
{

    private static final long serialVersionUID = 1L;

    private int                          gbfield;
    private Type                         gbfieldtype;
    private int                          afield;
    private Op                           op;
    private Map<Field, ArrayList<Tuple>> groupby_tuples;
    private ArrayList<Tuple>             nogroupby_tuples;
    private boolean groupby = false;
    private String    groupName;
    private TupleDesc td;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public
    IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what)
    {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;

        this.groupby_tuples = new HashMap<Field, ArrayList<Tuple>>();
        this.nogroupby_tuples = new ArrayList<Tuple>();
        this.groupName = null;
        this.td = null;
        if (gbfield != Aggregator.NO_GROUPING)
        { this.groupby = true; }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public
    void mergeTupleIntoGroup(Tuple tup)
    {
        // some code goes here
        setGroupName(tup);
        setTD(tup);

        if (groupby)
        {
            Field groupby_value = tup.getField(gbfield);

            if (!groupby_tuples.containsKey(groupby_value))
            {
                groupby_tuples.put(groupby_value, new ArrayList<Tuple>());
            }
            groupby_tuples.get(groupby_value).add(tup);
        }
        else
        {
            nogroupby_tuples.add(tup);
        }
    }

    /**
     * helper function to set TupleDesc
     *
     * @param tup
     */
    private
    void setTD(Tuple tup)
    {
        if (td == null)
        {
            td = tup.getTupleDesc();
        }
    }

    /**
     * helper function to set group name
     *
     * @param tup
     */
    private
    void setGroupName(Tuple tup)
    {
        if (groupName == null && gbfieldtype != null)
        {
            groupName = tup.getTupleDesc().getFieldName(gbfield);
            // groupName was null in input tuples
            if (groupName == null)
            { groupName = "null"; }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public
    DbIterator iterator()
    {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for proj2");
        return new GroupByIterator();
    }

    private
    class GroupByIterator implements DbIterator
    {
        private Iterator it = null;
        private TupleDesc groupby_td;
        private TupleDesc nogroupby_td;
        private Tuple   next = null;
        private boolean open = false;
        private int nogroupby_access_count = 1;

        public GroupByIterator()
        {
            // group-by tupleDesc
            Type[]   groupby_typeAr = {gbfieldtype, Type.INT_TYPE};
            String[] groupby_nameAr = {groupName, "null"};
            groupby_td = new TupleDesc(groupby_typeAr, groupby_nameAr);

            // no group-by tupleDesc
            Type[]   nogroupby_typeAr = {Type.INT_TYPE};
            String[] nogroupby_nameAr = {"null"};
            nogroupby_td = new TupleDesc(nogroupby_typeAr, nogroupby_nameAr);
        }

        /**
         * return the next groupby tuple
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public
        Tuple fetchNext() throws DbException, TransactionAbortedException
        {
            Tuple next_tuple = null;
            if (groupby)
            {
                while (it != null && it.hasNext())
                {
                    Map.Entry pair = (Map.Entry) it.next();
                    next_tuple = getGroupByTuple(pair.getKey(), pair.getValue());
                    break;
                }
            }
            else
            {
                if (nogroupby_access_count > 0)
                {
                    next_tuple = getGroupByTuple(null, nogroupby_tuples);
                    nogroupby_access_count = 0;
                }
            }
            return next_tuple;
        }

        /**
         * helper function to return next tuple with aggregate value
         *
         * @param groupby_field
         * @param tuples
         * @return
         */
        private
        Tuple getGroupByTuple(Object f, Object t)
        {
            Field groupby_field = null;
            if (f != null)
            {
                groupby_field = (Field) f;
            }

            ArrayList<Tuple> tuples = (ArrayList<Tuple>) t;

            Tuple new_tuple = null;

            int aggregate_value = 0;
            int i, min, max, sum, count;
            switch (op)
            {
                case MIN:
//                    System.out.format("MIN .....\n");
                    min = ((IntField)tuples.get(0).getField(afield)).getValue();
                    for (i = 1; i < tuples.size(); i++)
                    {
                        if (((IntField)tuples.get(i).getField(afield)).getValue() < min)
                        { min = ((IntField)tuples.get(i).getField(afield)).getValue(); }
                    }
                    aggregate_value = min;
                    break;
                case MAX:
//                    System.out.format("MAX .....\n");
                    max = ((IntField)tuples.get(0).getField(afield)).getValue();
                    for (i = 1; i < tuples.size(); i++)
                    {
                        if (((IntField)tuples.get(i).getField(afield)).getValue() > max)
                        { max = ((IntField)tuples.get(i).getField(afield)).getValue(); }
                    }
                    aggregate_value = max;
                    break;
                case SUM:
//                    System.out.format("SUM .....\n");
                    sum = ((IntField)tuples.get(0).getField(afield)).getValue();
                    for (i = 1; i < tuples.size(); i++)
                    {
                        sum += ((IntField)tuples.get(i).getField(afield)).getValue();
                    }
                    aggregate_value = sum;
                    break;
                case AVG:
//                    System.out.format("AVG .....\n");
                    sum = ((IntField)tuples.get(0).getField(afield)).getValue();
                    count = tuples.size();
                    for (i = 1; i < tuples.size(); i++)
                    {
                        sum += ((IntField)tuples.get(i).getField(afield)).getValue();
                    }
                    aggregate_value = sum / count;
                    break;
                case COUNT:
//                    System.out.format("COUNT .....\n");
                    count = tuples.size();
                    aggregate_value = count;
                    break;
            }

            if (groupby)
            {
                new_tuple = new Tuple(groupby_td);
                new_tuple.setField(0, groupby_field);
                new_tuple.setField(1, new IntField(aggregate_value));
            }
            else
            {
                new_tuple = new Tuple(nogroupby_td);
                new_tuple.setField(0, new IntField(aggregate_value));
            }
            return new_tuple;
        }

        /**
         * return ture if fetchNext() finds next tuple
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public
        boolean hasNext() throws DbException, TransactionAbortedException
        {
            if (!this.open)
            { throw new IllegalStateException("Operator not yet open"); }

            if (next == null)
            { next = fetchNext(); }
            return next != null;
        }

        /**
         * return next tuple
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         * @throws NoSuchElementException
         */
        public
        Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
        {
            if (next == null)
            {
                next = fetchNext();
                if (next == null)
                { throw new NoSuchElementException(); }
            }

            Tuple result = next;
            next = null;
            return result;
        }

        /**
         * init iterator
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public
        void open() throws DbException, TransactionAbortedException
        {
//            System.out.format("IntegerAggregator open ...\n");
            this.open = true;

            if (groupby)
            {
                it = groupby_tuples.entrySet().iterator();
            }
            nogroupby_access_count = 1;
        }

        public
        void close()
        {
            next = null;
            this.open = false;
            it = null;
            nogroupby_access_count = 0;
        }

        public
        TupleDesc getTupleDesc()
        {
//            System.out.format("tuple desc: %s\n", groupby_td.toString());
            TupleDesc td = null;
            if (groupby)
                td = groupby_td;
            else
                td = nogroupby_td;
            return td;
        }

        /**
         * reset iterator to the begining
         * @throws DbException
         * @throws TransactionAbortedException
         */
        public
        void rewind() throws DbException, TransactionAbortedException
        {
            if (groupby)
            {
                it = groupby_tuples.entrySet().iterator();
            }
            nogroupby_access_count = 1;
        }
    }

}
