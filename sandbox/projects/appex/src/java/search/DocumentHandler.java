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
 *    "Apache Turbine" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Turbine", nor may "Apache" appear in their name, without
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
import org.apache.lucene.document.DateField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import search.util.IOUtils;
import search.contenthandler.FileContentHandler;
import search.contenthandler.ContentHandlerFactory;

/**
 * <p>
 * A document is the atomic unit used for indexing purposes. It consists of
 * metadata as well as its file contents. File contents are handled by {@link FileContentHandler}.
 * </p>
 * <p>
 * DocumentHandler creates the {@link org.apache.lucene.document.Document},
 * adds the standard fields to it, delegates to {@link FileContentHandler} to handle
 * file contents, then adds to the {@link org.apache.lucene.index.IndexWriter}.
 * </p>
 * <p>
 * The standard fields are:<br>
 * <ul>
 * <li>filePath : Full filesystem path to the document
 * <li>fileName : File name of the document
 * <li>fileLastModifiedDate : Date the file was last modified
 * <li>fileSize : Size of the file in bytes
 * <li>fileFormat : Extension of the file {@see com.marketingbright.core.util.IOUtils#getFileExtension}
 * </ul>
 * </p>
 *
 * @author <a href="mailto:kelvin@relevanz.com">Kelvin Tan</a>
 */
public class DocumentHandler
{
    public static final String[] STANDARD_SEARCH_FIELDS =
            {"filePath", "fileName", "fileLastModifiedDate", "fileSize", "fileFormat"};
    private static Category cat = Category.getInstance(DocumentHandler.class.getName());
    private static Map customFields;
    private static final String EMPTY_STRING = "";

    /**
     * Document object this DocumentHandler is handling.
     */
    private Document doc;

    /**
     * Parent Document (null if none).
     */
    private Document parentDoc;

    /**
     * IndexWriter to add this document to.
     */
    private IndexWriter writer;

    public static void setCustomFields(Map aCustomFields)
    {
        customFields = aCustomFields;
    }

    public DocumentHandler(IndexWriter writer)
    {
        this.writer = writer;
        doc = new Document();
    }

    public DocumentHandler(IndexWriter writer, Document parentDoc)
    {
        this(writer);
        this.parentDoc = parentDoc;
    }

    public void process(Map metadata) throws IOException
    {
        File contentFile = new File((String) metadata.get("filePath"));

        // add the standard fields
        doc.add(Field.Keyword("filePath", contentFile.toString()));
        doc.add(Field.Text("fileName", contentFile.getName()));
        doc.add(Field.Keyword("fileLastModifiedDate", DateField.timeToString(contentFile.lastModified())));
        doc.add(Field.Keyword("fileSize", String.valueOf(contentFile.length())));
        doc.add(Field.Text("fileFormat", IOUtils.getFileExtension(contentFile)));

        // check if this is a document from datasource where
        // custom fields need to be added
        if (parentDoc == null)
        {
            // add the custom fields
            for (Iterator it = customFields.keySet().iterator(); it.hasNext();)
            {
                String field = (String) it.next();
                String value = (String) metadata.get(field);
                String type = (String) customFields.get(field);
                addFieldToDoc(type, field, value);
            }
            // Add OBJECT_CLASS_FIELD and OBJECT_IDENTIFIER
            // to populate the result templates with the proper
            // objects
            doc.add(Field.UnIndexed(DataSource.OBJECT_CLASS,
                                    (String) metadata.get(DataSource.OBJECT_CLASS)));
            doc.add(Field.Text(DataSource.OBJECT_IDENTIFIER,
                               (String) metadata.get(DataSource.OBJECT_IDENTIFIER)));
        }
        else
        {
            for (Iterator it = customFields.keySet().iterator(); it.hasNext();)
            {
                String field = (String) it.next();
                String value = parentDoc.get(field);
                String type = (String) customFields.get(field);
                addFieldToDoc(type, field, value);
            }
            // Add OBJECT_CLASS_FIELD and OBJECT_IDENTIFIER
            // to populate the result templates with the proper
            // objects
            doc.add(Field.UnIndexed(DataSource.OBJECT_CLASS,
                                    parentDoc.get(DataSource.OBJECT_CLASS)));
            doc.add(Field.Text(DataSource.OBJECT_IDENTIFIER,
                               parentDoc.get(DataSource.OBJECT_IDENTIFIER)));
        }
        if (!metadata.containsKey("fileContents"))
        {
            String extension = IOUtils.getFileExtension(contentFile);
            FileContentHandler cHandler = ContentHandlerFactory.getContentHandler(extension);
            if (cHandler != null)
            {
                cHandler.parse(doc, contentFile);
                if (cHandler.isNested())
                {
                    List nestedData = cHandler.getNestedData();
                    cat.debug("Nested data list size:" + nestedData.size());
                    for (int i = 0; i < nestedData.size(); i++)
                    {
                        Map dataMap = (Map) nestedData.get(i);
                        DocumentHandler handler = new DocumentHandler(writer, doc);
                        handler.process(dataMap);
                    }
                }
            }
            else
            {
                cat.warn("FileContentHandler not found for " + contentFile.getName());
            }
        }
        else
            doc.add(Field.Text("fileContents", (String) metadata.get("fileContents")));
        addToWriter();
    }

    public void addToWriter() throws IOException
    {
        writer.addDocument(this.doc);
    }

    private void addFieldToDoc(String type, String field, String value)
    {
        if (value == null)
            value = EMPTY_STRING;
        if (type.equalsIgnoreCase(SearchConfiguration.TEXT_FIELD_TYPE))
            doc.add(Field.Text(field, value));
        else if (type.equalsIgnoreCase(SearchConfiguration.KEYWORD_FIELD_TYPE))
            doc.add(Field.Keyword(field, value));
        else if (type.equalsIgnoreCase(SearchConfiguration.UNINDEXED_FIELD_TYPE))
            doc.add(Field.UnIndexed(field, value));
        else if (type.equalsIgnoreCase(SearchConfiguration.UNSTORED_FIELD_TYPE))
            doc.add(Field.UnStored(field, value));
    }
}
