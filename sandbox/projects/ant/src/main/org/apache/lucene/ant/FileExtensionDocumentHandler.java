package org.apache.lucene.ant;

import java.io.File;
import org.apache.lucene.document.Document;

/**
 *  Decides which class used to create the Lucene Document
 *  object based on its file extension.
 *
 *@author     Erik Hatcher
 *@created    October 28, 2001
 *@todo Add dynamic file extension/classname mappings for
 *      extensibility
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
        }
        catch (java.io.IOException e) {
            throw new DocumentHandlerException(e);
        }

        return doc;
    }
}

