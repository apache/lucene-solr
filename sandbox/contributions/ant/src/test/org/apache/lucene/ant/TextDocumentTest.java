package org.apache.lucene.ant;

import java.io.IOException;

import org.apache.lucene.ant.DocumentTestCase;
import org.apache.lucene.ant.TextDocument;

public class TextDocumentTest extends DocumentTestCase
{
    public TextDocumentTest (String name) {
        super(name);
    }
    
    TextDocument doc;
    
    public void setUp() throws IOException {
        doc = new TextDocument(getFile("test.txt"));
    }
    
    public void testDoc() {
        assertEquals("Contents", "Test Contents", doc.getContents());
    }
    
    public void tearDown() {
        doc = null;
    }
}

