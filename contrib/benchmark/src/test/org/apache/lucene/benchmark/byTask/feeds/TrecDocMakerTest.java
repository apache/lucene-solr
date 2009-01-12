package org.apache.lucene.benchmark.byTask.feeds;

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

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Date;

import junit.framework.TestCase;

public class TrecDocMakerTest extends TestCase {

  /** A TrecDocMaker which works on a String and not files. */
  private static class StringableTrecDocMaker extends TrecDocMaker {
  
    private String docs = null;
    
    public StringableTrecDocMaker(String docs, boolean forever) {
      this.docs = docs;
      this.forever = forever;
    }
    
    protected void openNextFile() throws NoMoreDataException, Exception {
      if (reader != null) {
        if (!forever) {
          throw new NoMoreDataException();
        }
        ++iteration;
      }
      
      reader = new BufferedReader(new StringReader(docs));
    }
    
  }
  
  private void assertDocData(DocData dd, String expName, String expTitle, String expBody, Date expDate) {
    assertNotNull(dd);
    assertEquals(expName, dd.getName());
    assertEquals(expTitle, dd.getTitle());
    assertTrue(dd.getBody().indexOf(expBody) != -1);
    assertEquals(expDate, dd.getDate());
  }
  
  private void assertNoMoreDataException(StringableTrecDocMaker stdm) throws Exception {
    boolean thrown = false;
    try {
      stdm.getNextDocData();
    } catch (NoMoreDataException e) {
      thrown = true;
    }
    assertTrue("Expecting NoMoreDataException", thrown);
  }
  
  public void testOneDocument() throws Exception {
    String docs = "<DOC>\r\n" + 
                  "<DOCNO>TEST-000</DOCNO>\r\n" + 
                  "<DOCHDR>\r\n" + 
                  "http://lucene.apache.org.trecdocmaker.test\r\n" + 
                  "HTTP/1.1 200 OK\r\n" + 
                  "Date: Sun, 11 Jan 2009 08:00:00 GMT\r\n" + 
                  "Server: Apache/1.3.27 (Unix)\r\n" + 
                  "Last-Modified: Sun, 11 Jan 2009 08:00:00 GMT\r\n" + 
                  "Content-Length: 614\r\n" + 
                  "Connection: close\r\n" + 
                  "Content-Type: text/html\r\n" + 
                  "</DOCHDR>\r\n" + 
                  "<html>\r\n" + 
                  "\r\n" + 
                  "<head>\r\n" + 
                  "<title>\r\n" + 
                  "TEST-000 title\r\n" + 
                  "</title>\r\n" + 
                  "</head>\r\n" + 
                  "\r\n" + 
                  "<body>\r\n" + 
                  "TEST-000 text\r\n" + 
                  "\r\n" + 
                  "</body>\r\n" + 
                  "\r\n" + 
                  "</DOC>";
    StringableTrecDocMaker stdm = new StringableTrecDocMaker(docs, false);
    stdm.setHTMLParser(new DemoHTMLParser());
    
    DocData dd = stdm.getNextDocData();
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", stdm
        .parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));
    
    assertNoMoreDataException(stdm);
  }
  
  public void testTwoDocuments() throws Exception {
    String docs = "<DOC>\r\n" + 
                  "<DOCNO>TEST-000</DOCNO>\r\n" + 
                  "<DOCHDR>\r\n" + 
                  "http://lucene.apache.org.trecdocmaker.test\r\n" + 
                  "HTTP/1.1 200 OK\r\n" + 
                  "Date: Sun, 11 Jan 2009 08:00:00 GMT\r\n" + 
                  "Server: Apache/1.3.27 (Unix)\r\n" + 
                  "Last-Modified: Sun, 11 Jan 2009 08:00:00 GMT\r\n" + 
                  "Content-Length: 614\r\n" + 
                  "Connection: close\r\n" + 
                  "Content-Type: text/html\r\n" + 
                  "</DOCHDR>\r\n" + 
                  "<html>\r\n" + 
                  "\r\n" + 
                  "<head>\r\n" + 
                  "<title>\r\n" + 
                  "TEST-000 title\r\n" + 
                  "</title>\r\n" + 
                  "</head>\r\n" + 
                  "\r\n" + 
                  "<body>\r\n" + 
                  "TEST-000 text\r\n" + 
                  "\r\n" + 
                  "</body>\r\n" + 
                  "\r\n" + 
                  "</DOC>\r\n" +
                  "<DOC>\r\n" + 
                  "<DOCNO>TEST-001</DOCNO>\r\n" + 
                  "<DOCHDR>\r\n" + 
                  "http://lucene.apache.org.trecdocmaker.test\r\n" + 
                  "HTTP/1.1 200 OK\r\n" + 
                  "Date: Sun, 11 Jan 2009 08:01:00 GMT\r\n" + 
                  "Server: Apache/1.3.27 (Unix)\r\n" + 
                  "Last-Modified: Sun, 11 Jan 2008 08:01:00 GMT\r\n" + 
                  "Content-Length: 614\r\n" + 
                  "Connection: close\r\n" + 
                  "Content-Type: text/html\r\n" + 
                  "</DOCHDR>\r\n" + 
                  "<html>\r\n" + 
                  "\r\n" + 
                  "<head>\r\n" + 
                  "<title>\r\n" + 
                  "TEST-001 title\r\n" + 
                  "</title>\r\n" + 
                  "</head>\r\n" + 
                  "\r\n" + 
                  "<body>\r\n" + 
                  "TEST-001 text\r\n" + 
                  "\r\n" + 
                  "</body>\r\n" + 
                  "\r\n" + 
                  "</DOC>";
    StringableTrecDocMaker stdm = new StringableTrecDocMaker(docs, false);
    stdm.setHTMLParser(new DemoHTMLParser());
    
    DocData dd = stdm.getNextDocData();
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", stdm
        .parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));
    
    dd = stdm.getNextDocData();
    assertDocData(dd, "TEST-001_0", "TEST-001 title", "TEST-001 text", stdm
        .parseDate("Sun, 11 Jan 2009 08:01:00 GMT"));
    
    assertNoMoreDataException(stdm);
  }

  // If a Date: attribute is missing, make sure the document is not skipped, but
  // rather that null Data is assigned.
  public void testMissingDate() throws Exception {
    String docs = "<DOC>\r\n" + 
                  "<DOCNO>TEST-000</DOCNO>\r\n" + 
                  "<DOCHDR>\r\n" + 
                  "http://lucene.apache.org.trecdocmaker.test\r\n" + 
                  "HTTP/1.1 200 OK\r\n" + 
                  "Server: Apache/1.3.27 (Unix)\r\n" + 
                  "Last-Modified: Sun, 11 Jan 2009 08:00:00 GMT\r\n" + 
                  "Content-Length: 614\r\n" + 
                  "Connection: close\r\n" + 
                  "Content-Type: text/html\r\n" + 
                  "</DOCHDR>\r\n" + 
                  "<html>\r\n" + 
                  "\r\n" + 
                  "<head>\r\n" + 
                  "<title>\r\n" + 
                  "TEST-000 title\r\n" + 
                  "</title>\r\n" + 
                  "</head>\r\n" + 
                  "\r\n" + 
                  "<body>\r\n" + 
                  "TEST-000 text\r\n" + 
                  "\r\n" + 
                  "</body>\r\n" + 
                  "\r\n" + 
                  "</DOC>\r\n" +
                  "<DOC>\r\n" + 
                  "<DOCNO>TEST-001</DOCNO>\r\n" + 
                  "<DOCHDR>\r\n" + 
                  "http://lucene.apache.org.trecdocmaker.test\r\n" + 
                  "HTTP/1.1 200 OK\r\n" + 
                  "Date: Sun, 11 Jan 2009 08:01:00 GMT\r\n" + 
                  "Server: Apache/1.3.27 (Unix)\r\n" + 
                  "Last-Modified: Sun, 11 Jan 2009 08:01:00 GMT\r\n" + 
                  "Content-Length: 614\r\n" + 
                  "Connection: close\r\n" + 
                  "Content-Type: text/html\r\n" + 
                  "</DOCHDR>\r\n" + 
                  "<html>\r\n" + 
                  "\r\n" + 
                  "<head>\r\n" + 
                  "<title>\r\n" + 
                  "TEST-001 title\r\n" + 
                  "</title>\r\n" + 
                  "</head>\r\n" + 
                  "\r\n" + 
                  "<body>\r\n" + 
                  "TEST-001 text\r\n" + 
                  "\r\n" + 
                  "</body>\r\n" + 
                  "\r\n" + 
                  "</DOC>";
    StringableTrecDocMaker stdm = new StringableTrecDocMaker(docs, false);
    stdm.setHTMLParser(new DemoHTMLParser());

    DocData dd = stdm.getNextDocData();
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", null);
    
    dd = stdm.getNextDocData();
    assertDocData(dd, "TEST-001_0", "TEST-001 title", "TEST-001 text", stdm
        .parseDate("Sun, 11 Jan 2009 08:01:00 GMT"));
    
    assertNoMoreDataException(stdm);
  }

  // When a 'bad date' is input (unparsable date), make sure the DocData date is
  // assigned null.
  public void testBadDate() throws Exception {
    String docs = "<DOC>\r\n" + 
                  "<DOCNO>TEST-000</DOCNO>\r\n" + 
                  "<DOCHDR>\r\n" + 
                  "http://lucene.apache.org.trecdocmaker.test\r\n" + 
                  "HTTP/1.1 200 OK\r\n" + 
                  "Date: Bad Date\r\n" + 
                  "Server: Apache/1.3.27 (Unix)\r\n" + 
                  "Last-Modified: Sun, 11 Jan 2009 08:00:00 GMT\r\n" + 
                  "Content-Length: 614\r\n" + 
                  "Connection: close\r\n" + 
                  "Content-Type: text/html\r\n" + 
                  "</DOCHDR>\r\n" + 
                  "<html>\r\n" + 
                  "\r\n" + 
                  "<head>\r\n" + 
                  "<title>\r\n" + 
                  "TEST-000 title\r\n" + 
                  "</title>\r\n" + 
                  "</head>\r\n" + 
                  "\r\n" + 
                  "<body>\r\n" + 
                  "TEST-000 text\r\n" + 
                  "\r\n" + 
                  "</body>\r\n" + 
                  "\r\n" + 
                  "</DOC>";
    StringableTrecDocMaker stdm = new StringableTrecDocMaker(docs, false);
    stdm.setHTMLParser(new DemoHTMLParser());

    DocData dd = stdm.getNextDocData();
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", null);
    
    assertNoMoreDataException(stdm);
  }

  public void testForever() throws Exception {
    String docs = "<DOC>\r\n" + 
                  "<DOCNO>TEST-000</DOCNO>\r\n" + 
                  "<DOCHDR>\r\n" + 
                  "http://lucene.apache.org.trecdocmaker.test\r\n" + 
                  "HTTP/1.1 200 OK\r\n" + 
                  "Date: Sun, 11 Jan 2009 08:00:00 GMT\r\n" + 
                  "Server: Apache/1.3.27 (Unix)\r\n" + 
                  "Last-Modified: Sun, 11 Jan 2009 08:00:00 GMT\r\n" + 
                  "Content-Length: 614\r\n" + 
                  "Connection: close\r\n" + 
                  "Content-Type: text/html\r\n" + 
                  "</DOCHDR>\r\n" + 
                  "<html>\r\n" + 
                  "\r\n" + 
                  "<head>\r\n" + 
                  "<title>\r\n" + 
                  "TEST-000 title\r\n" + 
                  "</title>\r\n" + 
                  "</head>\r\n" + 
                  "\r\n" + 
                  "<body>\r\n" + 
                  "TEST-000 text\r\n" + 
                  "\r\n" + 
                  "</body>\r\n" + 
                  "\r\n" + 
                  "</DOC>";
    StringableTrecDocMaker stdm = new StringableTrecDocMaker(docs, true);
    stdm.setHTMLParser(new DemoHTMLParser());

    DocData dd = stdm.getNextDocData();
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", stdm
        .parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));
    
    // same document, but the second iteration changes the name.
    dd = stdm.getNextDocData();
    assertDocData(dd, "TEST-000_1", "TEST-000 title", "TEST-000 text", stdm
        .parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));

    // Don't test that NoMoreDataException is thrown, since the forever flag is
    // turned on.
  }

}
