/*
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
package org.apache.lucene.benchmark.byTask.feeds;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;

import org.apache.lucene.benchmark.byTask.feeds.TrecDocParser.ParsePathType;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TrecContentSourceTest extends LuceneTestCase {

  /** A TrecDocMaker which works on a String and not files. */
  private static class StringableTrecSource extends TrecContentSource {
  
    private String docs = null;
    
    public StringableTrecSource(String docs, boolean forever) {
      this.docs = docs;
      this.forever = forever;
    }
    
    @Override
    void openNextFile() throws NoMoreDataException, IOException {
      if (reader != null) {
        if (!forever) {
          throw new NoMoreDataException();
        }
        ++iteration;
      }
      
      reader = new BufferedReader(new StringReader(docs));
    }
    
    @Override
    public void setConfig(Config config) {
      htmlParser = new DemoHTMLParser();
    }
  }
  
  private void assertDocData(DocData dd, String expName, String expTitle,
                             String expBody, Date expDate)
      throws ParseException {
    assertNotNull(dd);
    assertEquals(expName, dd.getName());
    assertEquals(expTitle, dd.getTitle());
    assertTrue(dd.getBody().indexOf(expBody) != -1);
    Date date = dd.getDate() != null ? DateTools.stringToDate(dd.getDate()) : null;
    assertEquals(expDate, date);
  }
  
  private void assertNoMoreDataException(StringableTrecSource stdm) throws Exception {
    expectThrows(NoMoreDataException.class, () -> {
      stdm.getNextDocData(null);
    });
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
    StringableTrecSource source = new StringableTrecSource(docs, false);
    source.setConfig(null);

    DocData dd = source.getNextDocData(new DocData());
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", source
        .parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));
    
    assertNoMoreDataException(source);
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
                  "<meta name=\"date\" content=\"Tue&#44; 09 Dec 2003 22&#58;39&#58;08 GMT\">" +
                  "</head>\r\n" + 
                  "\r\n" + 
                  "<body>\r\n" + 
                  "TEST-001 text\r\n" + 
                  "\r\n" + 
                  "</body>\r\n" + 
                  "\r\n" + 
                  "</DOC>";
    StringableTrecSource source = new StringableTrecSource(docs, false);
    source.setConfig(null);

    DocData dd = source.getNextDocData(new DocData());
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", source
        .parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));
    
    dd = source.getNextDocData(dd);
    assertDocData(dd, "TEST-001_0", "TEST-001 title", "TEST-001 text", source
        .parseDate("Tue, 09 Dec 2003 22:39:08 GMT"));
    
    assertNoMoreDataException(source);
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
    StringableTrecSource source = new StringableTrecSource(docs, false);
    source.setConfig(null);

    DocData dd = source.getNextDocData(new DocData());
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", null);
    
    dd = source.getNextDocData(dd);
    assertDocData(dd, "TEST-001_0", "TEST-001 title", "TEST-001 text", source
        .parseDate("Sun, 11 Jan 2009 08:01:00 GMT"));
    
    assertNoMoreDataException(source);
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
    StringableTrecSource source = new StringableTrecSource(docs, false);
    source.setConfig(null);

    DocData dd = source.getNextDocData(new DocData());
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", null);
    
    assertNoMoreDataException(source);
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
    StringableTrecSource source = new StringableTrecSource(docs, true);
    source.setConfig(null);

    DocData dd = source.getNextDocData(new DocData());
    assertDocData(dd, "TEST-000_0", "TEST-000 title", "TEST-000 text", source
        .parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));
    
    // same document, but the second iteration changes the name.
    dd = source.getNextDocData(dd);
    assertDocData(dd, "TEST-000_1", "TEST-000 title", "TEST-000 text", source
        .parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));
    source.close();

    // Don't test that NoMoreDataException is thrown, since the forever flag is
    // turned on.
  }
  
  /** 
   * Open a trec content source over a directory with files of all trec path types and all
   * supported formats - bzip, gzip, txt. 
   */
  public void testTrecFeedDirAllTypes() throws Exception {
    Path dataDir =  createTempDir("trecFeedAllTypes");
    TestUtil.unzip(getDataInputStream("trecdocs.zip"), dataDir);
    TrecContentSource tcs = new TrecContentSource();
    Properties props = new Properties();
    props.setProperty("print.props", "false");
    props.setProperty("content.source.verbose", "false");
    props.setProperty("content.source.excludeIteration", "true");
    props.setProperty("docs.dir", dataDir.toRealPath().toString().replace('\\','/'));
    props.setProperty("trec.doc.parser", TrecParserByPath.class.getName());
    props.setProperty("content.source.forever", "false");
    tcs.setConfig(new Config(props));
    tcs.resetInputs();
    DocData dd = new DocData();
    int n = 0;
    boolean gotExpectedException = false;
    HashSet<ParsePathType> unseenTypes = new HashSet<>(Arrays.asList(ParsePathType.values()));
    try {
      while (n<100) { // arbiterary limit to prevent looping forever in case of test failure
        dd = tcs.getNextDocData(dd);
        ++n;
        assertNotNull("doc data "+n+" should not be null!", dd);
        unseenTypes.remove(tcs.currPathType);
        switch(tcs.currPathType) {
          case GOV2:
            assertDocData(dd, "TEST-000", "TEST-000 title", "TEST-000 text", tcs.parseDate("Sun, 11 Jan 2009 08:00:00 GMT"));
            break;
          case FBIS:
            assertDocData(dd, "TEST-001", "TEST-001 Title", "TEST-001 text", tcs.parseDate("1 January 1991"));
            break;
          case FR94:
            // no title extraction in this source for now
            assertDocData(dd, "TEST-002", null, "DEPARTMENT OF SOMETHING", tcs.parseDate("February 3, 1994"));
            break;
          case FT:
            assertDocData(dd, "TEST-003", "Test-003 title", "Some pub text", tcs.parseDate("980424"));
            break;
          case LATIMES:
            assertDocData(dd, "TEST-004", "Test-004 Title", "Some paragraph", tcs.parseDate("January 17, 1997, Sunday"));
            break;
          default:
            assertTrue("Should never get here!", false);
        }
      }
    } catch (NoMoreDataException e) {
      gotExpectedException = true;
    }
    assertTrue("Should have gotten NoMoreDataException!", gotExpectedException);
    assertEquals("Wrong number of documents created by source!",5,n);
    assertTrue("Did not see all types!",unseenTypes.isEmpty());
  }

}
