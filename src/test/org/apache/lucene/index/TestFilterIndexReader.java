package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001, 2002, 2003 The Apache Software Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */


import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import junit.framework.TestResult;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.util.Collection;
import java.io.IOException;

public class TestFilterIndexReader extends TestCase {

  private static class TestReader extends FilterIndexReader {

     /** Filter that only permits terms containing 'e'.*/
    private static class TestTermEnum extends FilterTermEnum {
      public TestTermEnum(TermEnum enum)
        throws IOException {
        super(enum);
      }

      /** Scan for terms containing the letter 'e'.*/
      public boolean next() throws IOException {
        while (in.next()) {
          if (in.term().text().indexOf('e') != -1)
            return true;
        }
        return false;
      }
    }
    
    /** Filter that only returns odd numbered documents. */
    private static class TestTermPositions extends FilterTermPositions {
      public TestTermPositions(TermPositions in)
        throws IOException {
        super(in);
      }

      /** Scan for odd numbered documents. */
      public boolean next() throws IOException {
        while (in.next()) {
          if ((in.doc() % 2) == 1)
            return true;
        }
        return false;
      }
    }
    
    public TestReader(IndexReader reader) {
      super(reader);
    }

    /** Filter terms with TestTermEnum. */
    public TermEnum terms() throws IOException {
      return new TestTermEnum(in.terms());
    }

    /** Filter positions with TestTermPositions. */
    public TermPositions termPositions() throws IOException {
      return new TestTermPositions(in.termPositions());
    }
  }


  /** Main for running test case by itself. */
  public static void main(String args[]) {
    TestRunner.run (new TestSuite(TestIndexReader.class));
  }
    
  /**
   * Tests the IndexReader.getFieldNames implementation
   * @throws Exception on error
   */
  public void testFilterIndexReader() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer =
      new IndexWriter(directory, new WhitespaceAnalyzer(), true);

    Document d1 = new Document();
    d1.add(Field.Text("default","one two"));
    writer.addDocument(d1);

    Document d2 = new Document();
    d2.add(Field.Text("default","one three"));
    writer.addDocument(d2);

    Document d3 = new Document();
    d3.add(Field.Text("default","two four"));
    writer.addDocument(d3);

    writer.close();

    IndexReader reader = new TestReader(IndexReader.open(directory));

    TermEnum terms = reader.terms();
    while (terms.next()) {
      assertTrue(terms.term().text().indexOf('e') != -1);
    }
    terms.close();
    
    TermPositions positions = reader.termPositions(new Term("default", "one"));
    while (positions.next()) {
      assertTrue((positions.doc() % 2) == 1);
    }

    reader.close();
  }
}
