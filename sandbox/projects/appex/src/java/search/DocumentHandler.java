package search;

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

import org.apache.log4j.Category;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import search.util.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * <p>
 * A document is the atomic unit used for indexing purposes. It consists of
 * metadata as well as its file contents. File contents are handled by
 * {@link ContentHandler}.
 * </p>
 * <p>
 * DocumentHandler creates the {@link org.apache.lucene.document.Document},
 * adds fields to it, delegates to {@link ContentHandler} to handle
 * file contents.
 * </p>
 */
public class DocumentHandler
{
    /**
     * Field to retrieve all documents.
     */
    public static final String ALL_DOCUMENTS_FIELD = "AllDocuments";

    private static Category cat = Category.getInstance(DocumentHandler.class);

    private static boolean isDebugEnabled = cat.isDebugEnabled();

    /**
     * Should parent documents include data of its children?
     */
    private static boolean parentEncapsulation = false;
    /**
     * Document object this DocumentHandler is handling.
     */
    private Document doc;

    /**
     * Map of metadata for this document. Contains the field:value pair
     * to be added to the document.
     */
    private Map metadata;

    /**
     * Map of fields. Contains field:type_of_field pair.
     */
    private Map customFields;

    /**
     * IndexWriter.
     */
    private IndexWriter writer;

    /**
     * A collection of documents to be added to the writer.
     */
    private List documents = new ArrayList();

    /**
     * Ctor.
     *
     * @param Map of metadata for this document.
     * @param Map of fields.
     * @param Writer.
     */
    public DocumentHandler(Map metadata,
                           Map customFields,
                           IndexWriter writer)
    {
        this.metadata = metadata;
        this.customFields = customFields;
        this.writer = writer;
    }

    /**
     * Handles the actual processing of the document.
     */
    public void process() throws IOException, Exception
    {
        String objectid = (String) metadata.get(DataSource.OBJECT_IDENTIFIER);
        if (objectid == null)
            return;
        doc = createDocument();
        addMapToDoc(metadata);
        addNestedDataSource(metadata);
        doc.add(Field.Text(ALL_DOCUMENTS_FIELD, ALL_DOCUMENTS_FIELD));
        //documents.add(doc);
        if (writer != null)
        {
            addToWriter();
        }
        else
        {
            documents.add(doc);
        }
    }

    private List getDocuments()
    {
        return documents;
    }

    private Document createDocument()
    {
        return new Document();
    }

    /**
     * Add the contents of a Map to a document.
     *
     * @param Map to add.
     */
    private void addMapToDoc(Map map)
    {
        for (Iterator it = map.keySet().iterator(); it.hasNext();)
        {
            String field = (String) it.next();
            Object value = map.get(field);
            if (value instanceof String)
            {
                String type = null;
                if (customFields != null)
                {
                    type = (String) customFields.get(field);
                }
                addFieldToDoc(type, field, (String) value);
            }
            else if (value instanceof Reader)
            {
                addFieldToDoc(field, (Reader) value);
            }
        }
    }

    /**
     * Add nested datasources.
     *
     * @param Map which contains the nested datasources.
     */
    private void addNestedDataSource(Map map) throws Exception
    {
        Object o = map.get(DataSource.NESTED_DATASOURCE);
        if (o == null)
            return;
        if (o instanceof List)
        {
            List nestedDataSource = (List) o;
            for (int i = 0; i < nestedDataSource.size(); i++)
            {
                DataSource ds = (DataSource) nestedDataSource.get(i);
                addDataSource(ds);
            }
        }
        else if (o instanceof DataSource)
        {
            DataSource ds = (DataSource) o;
            addDataSource(ds);
        }
    }

    /**
     * Datasources are basically a collection of data maps to be indexed.
     * addMapToDoc is invoked for each map.
     *
     * @param Datasource to add.
     */
    private void addDataSource(DataSource ds) throws Exception
    {
        Map[] data = ds.getData();
        for (int i = 0; i < data.length; i++)
        {
            Map map = data[i];
            if (map.containsKey(DataSource.OBJECT_IDENTIFIER))
            {
                /**
                 * Create a new document because child datasources may need
                 * to be retrieved independently of parent doc.
                 */
                DocumentHandler docHandler = new DocumentHandler(map, null, null);
                docHandler.process();
                documents.addAll(docHandler.getDocuments());
            }
            else
            {
                addMapToDoc(map);
                /**
                 * Add nested datasources of this datasource's data
                 */
                addNestedDataSource(map);
            }
        }
    }

    /**
     * Adds a String-based field to a document.
     *
     * @param Type of field.
     * @param Name of field.
     * @param Value of field.
     */
    private void addFieldToDoc(String type, String field, String value)
    {
        if (value == null)
            value = StringUtils.EMPTY_STRING;
        if (SearchConfiguration.KEYWORD_FIELD_TYPE.equalsIgnoreCase(type))
            doc.add(Field.Keyword(field, value));
        else if (SearchConfiguration.UNINDEXED_FIELD_TYPE.equalsIgnoreCase(type))
            doc.add(Field.UnIndexed(field, value));
        else if (SearchConfiguration.UNSTORED_FIELD_TYPE.equalsIgnoreCase(type))
            doc.add(Field.UnStored(field, value));
        else
            doc.add(Field.Text(field, value));
    }

    /**
     * Adds a Reader-based field to a document.
     *
     * @param Name of field.
     * @param Reader.
     */
    private void addFieldToDoc(String field, Reader reader)
    {
        doc.add(Field.Text(field, reader));
    }

    /**
     * Adds documents to the IndexWriter.
     */
    private void addToWriter() throws IOException
    {
        if (parentEncapsulation)
        {
            for (int i = 0; i < documents.size(); i++)
            {
                Document d = (Document) documents.get(i);
                for (Enumeration e = d.fields(); e.hasMoreElements();)
                {
                    Field f = (Field) e.nextElement();
                    String fieldName = f.name();
                    if (!fieldName.equals(DataSource.CONTAINER_IDENTIFIER)
                            && !fieldName.equals(DataSource.OBJECT_CLASS)
                            && !fieldName.equals(DataSource.OBJECT_IDENTIFIER))
                    {
                        doc.add(f);
                    }
                }
            }
        }
        writer.addDocument(doc);
        for (int i = 0; i < documents.size(); i++)
        {
            writer.addDocument((Document) documents.get(i));
        }
        //cat.debug((documents.size() + 1) + " documents added.");
    }
}
