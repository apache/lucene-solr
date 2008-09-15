package org.apache.lucene.ant;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.document.Document;

import java.io.File;

/**
 *  A DocumentHandler implementation to delegate responsibility to
 *  based on a files extension.  Currently only .html and .txt
 *  files are handled, other extensions ignored.
 *
 *@since      October 28, 2001
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

