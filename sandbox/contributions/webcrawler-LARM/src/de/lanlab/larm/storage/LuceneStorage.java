package de.lanlab.larm.storage;

import de.lanlab.larm.util.WebDocument;

/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author
 * @version   1.0
 */
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import java.util.*;
import java.io.*;

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   14. Juni 2002
 */
public class LuceneStorage implements DocumentStorage
{

    HashMap fieldInfos = new HashMap();
    IndexWriter writer;
    Analyzer analyzer;
    String indexName;


    /**
     * Constructor for the LuceneStorage object
     */
    public LuceneStorage() { }


    /**
     * Sets the analyzer attribute of the LuceneStorage object
     *
     * @param a  The new analyzer value
     */
    public void setAnalyzer(Analyzer a)
    {
        this.analyzer = a;
    }


    /**
     * Sets the indexName attribute of the LuceneStorage object
     *
     * @param name  The new indexName value
     */
    public void setIndexName(String name)
    {
        this.indexName = name;
    }


    /**
     * Sets the fieldInfo attribute of the LuceneStorage object
     *
     * @param fieldName  The new fieldInfo value
     * @param value      The new fieldInfo value
     */
    public void setFieldInfo(String fieldName, int value)
    {
        fieldInfos.put(fieldName, new Integer(value));
    }


    /**
     * Sets the create attribute of the LuceneStorage object
     *
     * @param create  The new create value
     */
    public void setCreate(boolean create)
    {
        this.create = create;
    }

    boolean create;


    /**
     * Description of the Method
     */
    public void open()
    {
        System.out.println("opening Lucene storage with index name " + indexName + ")");
        try
        {
            writer = new IndexWriter(indexName, analyzer, create);
        }
        catch(IOException e)
        {
            System.err.println("IOException occured when opening Lucene Index with index name '" + indexName + "'");
            e.printStackTrace();
        }
        if(writer != null)
        {
            System.out.println("lucene storage opened successfully");
        }
    }


    public final static int INDEX = 1;
    public final static int STORE = 2;
    public final static int TOKEN = 4;


    /**
     * Gets the fieldInfo attribute of the LuceneStorage object
     *
     * @param fieldName  Description of the Parameter
     * @param def        Description of the Parameter
     * @return           The fieldInfo value
     */
    protected int getFieldInfo(String fieldName, int def)
    {
        Integer info = (Integer) fieldInfos.get(fieldName);
        if (info != null)
        {
            return info.intValue();
        }
        else
        {
            return def;
        }
    }


    protected void addField(Document doc, String name, String value, int defaultIndexFlags)
    {
        int flags = getFieldInfo(name, defaultIndexFlags);
        if (flags != 0)
        {
            doc.add(new Field(name, value, (flags & STORE) != 0, (flags & INDEX) != 0, (flags & TOKEN) != 0));
        }
    }

    /**
     * Description of the Method
     *
     * @param webDoc  Description of the Parameter
     * @return        Description of the Return Value
     */
    public WebDocument store(WebDocument webDoc)
    {
        //System.out.println("storing " + webDoc.getUrl());
        boolean store;
        boolean index;
        boolean token;
        store = index = token = false;

        Document doc = new Document();
        int flags;

        addField(doc, "url", webDoc.getUrl().toExternalForm(), STORE | INDEX);
        addField(doc, "mimetype", webDoc.getMimeType(), STORE | INDEX);
        // addField(doc, "...", webDoc.getNormalizedURLString(), STORE | INDEX); and so fortg
        // todo: other fields
        Set fields = webDoc.getFieldNames();

        for (Iterator it = fields.iterator(); it.hasNext(); )
        {
            String fieldName = (String) it.next();
            Object field = webDoc.getField(fieldName);

            if (field instanceof char[])
            {
                addField(doc, fieldName, new String((char[]) field), STORE | INDEX);
            }
            else if (field instanceof String)
            {
                addField(doc, fieldName, (String)field, STORE | INDEX);
            }
            /* else ? */
        }
        try
        {
            writer.addDocument(doc);
        }
        catch(IOException e)
        {
            System.err.println("IOException occured when adding document to Lucene index");
            e.printStackTrace();
        }
        return webDoc;
    }

    //public void set
}
