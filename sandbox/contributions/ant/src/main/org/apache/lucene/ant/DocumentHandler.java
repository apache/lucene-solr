package org.apache.lucene.ant;

import java.io.File;
import org.apache.lucene.document.Document;

/**
 *  Allows a class to act as a Lucene document handler
 *
 *@author     Erik Hatcher
 *@created    October 27, 2001
 */
public interface DocumentHandler {
    /**
     *  Gets the document attribute of the DocumentHandler object
     *
     *@param  file  Description of Parameter
     *@return       The document value
     *@throws DocumentHandlerException
     */
    public Document getDocument(File file)
                                  throws DocumentHandlerException;
}

