package org.apache.lucene.ant;

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
 *@author     Erik Hatcher
 *@created    December 6, 2001
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

        doc.add(Field.Text("title", f.getName()));
        doc.add(Field.Text("contents", textDoc.getContents()));
        doc.add(Field.UnIndexed("rawcontents",
                textDoc.getContents()));

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

