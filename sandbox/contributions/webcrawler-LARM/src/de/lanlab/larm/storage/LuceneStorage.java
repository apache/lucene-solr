/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

package de.lanlab.larm.storage;

import de.lanlab.larm.util.WebDocument;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import java.util.*;
import java.io.*;

/**
 * FIXME document this class
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author    Administrator
 * @created   14. Juni 2002
 * @version $Id$
 */
public class LuceneStorage implements DocumentStorage
{
    public final static int INDEX = 1;
    public final static int STORE = 2;
    public final static int TOKEN = 4;

    private HashMap fieldInfos = new HashMap();
    private IndexWriter writer;
    private Analyzer analyzer;
    private String indexName;
    private boolean create;


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


    /**
     * Description of the Method
     */
    public void open()
    {
	// FIXME: replace with logging
        System.out.println("opening Lucene storage with index name " + indexName + ")");
        try
        {
            writer = new IndexWriter(indexName, analyzer, create);
        }
        catch(IOException e)
        {
	    // FIXME: replace with logging
            System.err.println("IOException occured when opening Lucene Index with index name '" + indexName + "'");
            e.printStackTrace();
        }
        if (writer != null)
        {
	    // FIXME: replace with logging
            System.out.println("lucene storage opened successfully");
        }
    }


    /**
     * Gets the fieldInfo attribute of the LuceneStorage object
     *
     * @param fieldName  Description of the Parameter
     * @param defaultValue Description of the Parameter
     * @return           The fieldInfo value
     */
    protected int getFieldInfo(String fieldName, int defaultValue)
    {
        Integer info = (Integer) fieldInfos.get(fieldName);
        if (info != null)
        {
            return info.intValue();
        }
        else
        {
            return defaultValue;
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
        boolean store = false;
        boolean index = false;
        boolean token = false;

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
	    // FIXME: replace with logging
            System.err.println("IOException occured when adding document to Lucene index");
            e.printStackTrace();
        }
        return webDoc;
    }

    //public void set
}
