package org.apache.lucene.ant;

import org.apache.lucene.document.Document;

import java.io.File;

/**
 *  A DocumentHandler implementation to delegate responsibility to
 *  based on a files extension.  Currently only .html and .txt
 *  files are handled, other extensions ignored.
 *
 *@author     Erik Hatcher
 *@created    October 28, 2001
 *@todo Implement dynamic document type lookup
 */
public class FileExtensionDocumentHandler
        implements DocumentHandler {
    /**
     *  Gets the document attribute of the
     *  FileExtensionDocumentHandler object
     *
     *@param  file                          Description of
     *      Parameter
     *@return                               The document value
     *@exception  DocumentHandlerException  Description of
     *      Exception
     */
    public Document getDocument(File file)
            throws DocumentHandlerException {
        Document doc = null;

        String name = file.getName();

        try {
            if (name.endsWith(".txt")) {
                doc = TextDocument.Document(file);
            }

            if (name.endsWith(".html")) {
                doc = HtmlDocument.Document(file);
            }
        } catch (java.io.IOException e) {
            throw new DocumentHandlerException(e);
        }

        return doc;
    }
}

