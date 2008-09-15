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
import org.apache.lucene.document.Field;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;

/**
 *  A utility for making Lucene Documents from a File.
 *
 *@since      December 6, 2001
 *@todo       Fix JavaDoc comments here
 */

public class TextDocument {
    private String contents;


    /**
     *  Constructor for the TextDocument object
     *
     *@param  file             Description of Parameter
     *@exception  IOException  Description of Exception
     */
    public TextDocument(File file) throws IOException {
        BufferedReader br =
                new BufferedReader(new FileReader(file));
        StringWriter sw = new StringWriter();

        String line = br.readLine();
        while (line != null) {
            sw.write(line);
            line = br.readLine();
        }
        br.close();

        contents = sw.toString();
        sw.close();
    }


    /**
     *  Makes a document for a File. <p>
     *
     *  The document has a single field:
     *  <ul>
     *    <li> <code>contents</code>--containing the full contents
     *    of the file, as a Text field;
     *
     *@param  f                Description of Parameter
     *@return                  Description of the Returned Value
     *@exception  IOException  Description of Exception
     */
    public static Document Document(File f) throws IOException {

        TextDocument textDoc = new TextDocument(f);
        // make a new, empty document
        Document doc = new Document();

        doc.add(new Field("title", f.getName(), Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("contents", textDoc.getContents(), Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field("rawcontents", textDoc.getContents(), Field.Store.YES, Field.Index.NO));

        // return the document
        return doc;
    }


    /**
     *@return    The contents value
     *@todo      finish this method
     */
    public String getContents() {
        return contents;
    }
}

