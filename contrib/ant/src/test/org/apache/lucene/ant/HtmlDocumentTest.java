package org.apache.lucene.ant;

import java.io.IOException;

import org.apache.lucene.ant.DocumentTestCase;
import org.apache.lucene.ant.HtmlDocument;

public class HtmlDocumentTest extends DocumentTestCase
{
    public HtmlDocumentTest (String name) {
        super(name);
    }
    
    HtmlDocument doc;
    
    public void setUp() throws IOException {
        doc = new HtmlDocument(getFile("test.html"));
    }
    
    public void testDoc() {
        assertEquals("Title", "Test Title", doc.getTitle());
        assertTrue("Body", doc.getBody().startsWith("This is some test"));
    }
    
    public void tearDown() {
        doc = null;
    }
}

