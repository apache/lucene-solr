package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
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
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;

import java.io.IOException;

/**
 * @author schnee
 * @version $Id$
 **/

public class TestBooleanPrefixQuery extends TestCase {

  public static void main(String[] args) {
    TestRunner.run(suite());
  }

  public static Test suite() {
    return new TestSuite(TestBooleanPrefixQuery.class);
  }

  public TestBooleanPrefixQuery(String name) {
    super(name);
  }

  public void testMethod() {
    RAMDirectory directory = new RAMDirectory();

    String[] categories = new String[]{"food",
                                       "foodanddrink",
                                       "foodanddrinkandgoodtimes",
                                       "food and drink"};

    Query rw1 = null;
    Query rw2 = null;
    try {
      IndexWriter writer = new IndexWriter(directory, new
                                           WhitespaceAnalyzer(), true);
      for (int i = 0; i < categories.length; i++) {
        Document doc = new Document();
        doc.add(Field.Keyword("category", categories[i]));
        writer.addDocument(doc);
      }
      writer.close();
      
      IndexReader reader = IndexReader.open(directory);
      PrefixQuery query = new PrefixQuery(new Term("category", "foo"));
      
      rw1 = query.rewrite(reader);
      
      BooleanQuery bq = new BooleanQuery();
      bq.add(query, true, false);
      
      rw2 = bq.rewrite(reader);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    BooleanQuery bq1 = null;
    if (rw1 instanceof BooleanQuery) {
      bq1 = (BooleanQuery) rw1;
    }

    BooleanQuery bq2 = null;
    if (rw2 instanceof BooleanQuery) {
        bq2 = (BooleanQuery) rw2;
    } else {
      fail("Rewrite");
    }

    assertEquals("Number of Clauses Mismatch", bq1.getClauses().length,
                 bq2.getClauses().length);
  }
}

