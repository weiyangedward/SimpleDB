package simpledb;

import com.sun.org.apache.bcel.internal.generic.F2D;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and
 * their associated schemas.
 * For now, this is a stub catalog that must be populated with tables
 * by a user program before it can be used -- eventually, this should
 * be converted to a catalog that reads a catalog table from disk.
 */

public
class Catalog
{
    private ArrayList<DbFile> tables;
    private ArrayList<String> table_names;
    private ArrayList<String> primary_keys;
    private int               N; // num of tables added

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public
    Catalog()
    {
        // some code goes here
        tables = new ArrayList<DbFile>(); // table files
        table_names = new ArrayList<String>(); // table names
        primary_keys = new ArrayList<String>(); // primary keys of tables
        N = 0;
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;
     *                  file.getId() is the identfier of this
     *                  file/tupledesc param
     *                  for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.
     *                  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     *                  <p>
     *                  conflict exists, use the last table to be added
     *                  as the table for a given name.
     */
    public
    void addTable(DbFile file, String name, String pkeyField)
    {
        // some code goes here
        tables.add(file);
        table_names.add(name);
        primary_keys.add(pkeyField);
        N++;
    }

    public
    void addTable(DbFile file, String name)
    {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc
     * and its contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is
     *             the identfier of this file/tupledesc param for the
     *             calls getTupleDesc and getFile
     */
    public
    void addTable(DbFile file)
    {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public
    int getTableId(String name) throws NoSuchElementException
    {
        // some code goes here
        if (name == null) throw new NoSuchElementException("name is null");
        int index = table_names.indexOf(name);
        if (index != -1)
            return tables.get(index).getId();
        throw new NoSuchElementException("name not found");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the
     *                DbFile.getId() function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public
    TupleDesc getTupleDesc(int tableid) throws NoSuchElementException
    {
        // some code goes here
        for (int i=0; i<N; i++)
        {
            if (tables.get(i).getId() == tableid)
                return tables.get(i).getTupleDesc();
        }
        throw new NoSuchElementException(tableid + " not valid");
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile
     *                .getId() function passed to addTable
     */
    public
    DbFile getDbFile(int tableid) throws NoSuchElementException
    {
        // some code goes here
//        System.out.format("input table id: %d\n", tableid);
        for (int i=0; i<N; i++)
        {
//            System.out.format("exists table id: %d\n", tables.get(i).getId());
            if (tables.get(i).getId() == tableid)
                return tables.get(i);
        }
        throw new NoSuchElementException(tableid + " not valid");
    }

    public
    String getPrimaryKey(int tableid)
    {
        // some code goes here
        for (int i=0; i<N; i++)
        {
            if (tables.get(i).getId() == tableid)
                return primary_keys.get(i);
        }
        throw new NoSuchElementException(tableid + " not valid");
    }

    public
    Iterator<Integer> tableIdIterator()
    {
        // some code goes here
        return new tableIterator();
    }

    private class tableIterator implements Iterator<Integer>
    {
        private int i=0;

        public boolean  hasNext()
        {
            return i < N;
        }

        public Integer next()
        {
            if (!hasNext()) throw new NoSuchElementException("table underflow");
            Integer tableId = new Integer(tables.get(i).getId());
            i++;
            return tableId;
        }

        public void remove()
        {
            throw new UnsupportedOperationException("remove in iterator is not allowed");
        }
    }

    public
    String getTableName(int id)
    {
        // some code goes here
        for (int i=0; i<N; i++)
        {
            if (tables.get(i).getId() == id)
                return table_names.get(i);
        }
        throw new NoSuchElementException(id + " not valid");
    }

    /**
     * Delete all tables from the catalog
     */
    public
    void clear()
    {
        // some code goes here
        tables.clear();
        table_names.clear();
        primary_keys.clear();
        N = 0;
    }

    /**
     * Reads the schema from a file and creates the appropriate tables
     * in the database.
     *
     * @param catalogFile
     */
    public
    void loadSchema(String catalogFile)
    {
        String line       = "";
        String baseFolder = new File(catalogFile).getParent();
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null)
            {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String            fields     = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[]          els        = fields.split(",");
                ArrayList<String> names      = new ArrayList<String>();
                ArrayList<Type>   types      = new ArrayList<Type>();
                String            primaryKey = "";
                for (String e : els)
                {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                    {
                        types.add(Type.INT_TYPE);
                    }
                    else if (els2[1].trim().toLowerCase().equals("string"))
                    {
                        types.add(Type.STRING_TYPE);
                    }
                    else
                    {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3)
                    {
                        if (els2[2].trim().equals("pk"))
                        { primaryKey = els2[0].trim(); }
                        else
                        {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[]    typeAr  = types.toArray(new Type[0]);
                String[]  namesAr = names.toArray(new String[0]);
                TupleDesc t       = new TupleDesc(typeAr, namesAr);
                HeapFile  tabHf   = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.exit(0);
        }
        catch (IndexOutOfBoundsException e)
        {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

