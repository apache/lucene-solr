package org.apache.lucene;

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

import com.lucene.store.*;
import com.lucene.document.*;
import com.lucene.analysis.*;
import com.lucene.index.*;
import com.lucene.search.*;
import com.lucene.queryParser.*;

import java.io.File;
import java.util.Random;

class ThreadSafetyTest {
  private static final Analyzer ANALYZER = new SimpleAnalyzer();
  private static final Random RANDOM = new Random();
  private static Searcher SEARCHER;

  private static int random(int i) {		  // for JDK 1.1 compatibility
    int r = RANDOM.nextInt();
    if (r < 0) r = -r;
    return r % i;
  }

  private static class IndexerThread extends Thread {
    private final int reopenInterval = 30 + random(60);
    IndexWriter writer;

    public IndexerThread(IndexWriter writer) {
      this.writer = writer;
    }

    public void run() {
      try {
	for (int i = 0; i < 1024*16; i++) {
	  Document d = new Document();
	  int n = RANDOM.nextInt();
	  d.add(Field.Keyword("id", Integer.toString(n)));
	  d.add(Field.UnStored("contents", intToEnglish(n)));
	  System.out.println("Adding " + n);
	  writer.addDocument(d);

	  if (i%reopenInterval == 0) {
	    writer.close();
	    writer = new IndexWriter("index", ANALYZER, false);
	  }
	}
      } catch (Exception e) {
	System.out.println(e.toString());
	e.printStackTrace();
	System.exit(0);
      }
    }
  }

  private static class SearcherThread extends Thread {
    private IndexSearcher searcher;
    private final int reopenInterval = 10 + random(20);

    public SearcherThread(boolean useGlobal) throws java.io.IOException {
      if (!useGlobal)
	this.searcher = new IndexSearcher("index");
    }

    public void run() {
      try {
	for (int i = 0; i < 1024*8; i++) {
	  searchFor(RANDOM.nextInt(), (searcher==null)?SEARCHER:searcher);
	  if (i%reopenInterval == 0) {
	    if (searcher == null) {
	      SEARCHER = new IndexSearcher("index");
	    } else {
	      searcher.close();
	      searcher = new IndexSearcher("index");
	    }
	  }
	}
      } catch (Exception e) {
	System.out.println(e.toString());
	e.printStackTrace();
	System.exit(0);
      }
    }

    private void searchFor(int n, Searcher searcher)
      throws Exception {
      System.out.println("Searching for " + n);
      Hits hits =
	searcher.search(QueryParser.parse(intToEnglish(n), "contents",
					  ANALYZER));
      System.out.println("Search for " + n + ": total=" + hits.length());
      for (int j = 0; j < Math.min(3, hits.length()); j++) {
	System.out.println("Hit for " + n + ": " + hits.doc(j).get("id"));
      }
    }
  }

  public static void main(String[] args) throws Exception {

    IndexWriter writer = new IndexWriter("index", ANALYZER, true);

    Thread indexerThread = new IndexerThread(writer);
    indexerThread.start();

    Thread.sleep(1000);
      
    SearcherThread searcherThread1 = new SearcherThread(false);
    searcherThread1.start();

    SEARCHER = new IndexSearcher("index");

    SearcherThread searcherThread2 = new SearcherThread(true);
    searcherThread2.start();

    SearcherThread searcherThread3 = new SearcherThread(true);
    searcherThread3.start();
  }

  private static String intToEnglish(int i) {
    StringBuffer result = new StringBuffer();
    intToEnglish(i, result);
    return result.toString();
  }

  private static void intToEnglish(int i, StringBuffer result) {
    if (i < 0) {
      result.append("minus ");
      i = -i;
    }
    if (i >= 1000000000) {			  // billions
      intToEnglish(i/1000000000, result);
      result.append("billion, ");
      i = i%1000000000;
    }
    if (i >= 1000000) {				  // millions
      intToEnglish(i/1000000, result);
      result.append("million, ");
      i = i%1000000;
    }
    if (i >= 1000) {				  // thousands
      intToEnglish(i/1000, result);
      result.append("thousand, ");
      i = i%1000;
    }
    if (i >= 100) {				  // hundreds
      intToEnglish(i/100, result);
      result.append("hundred ");
      i = i%100;
    }
    if (i >= 20) {
      switch (i/10) {
      case 9 : result.append("ninety"); break;
      case 8 : result.append("eighty"); break;
      case 7 : result.append("seventy"); break;
      case 6 : result.append("sixty"); break;
      case 5 : result.append("fifty"); break;
      case 4 : result.append("forty"); break;
      case 3 : result.append("thirty"); break;
      case 2 : result.append("twenty"); break;
      }
      i = i%10;
      if (i == 0)
	result.append(" ");
      else 
	result.append("-");
    }
    switch (i) {
    case 19 : result.append("nineteen "); break;
    case 18 : result.append("eighteen "); break;
    case 17 : result.append("seventeen "); break;
    case 16 : result.append("sixteen "); break;
    case 15 : result.append("fifteen "); break;
    case 14 : result.append("fourteen "); break;
    case 13 : result.append("thirteen "); break;
    case 12 : result.append("twelve "); break;
    case 11 : result.append("eleven "); break;
    case 10 : result.append("ten "); break;
    case 9 : result.append("nine "); break;
    case 8 : result.append("eight "); break;
    case 7 : result.append("seven "); break;
    case 6 : result.append("six "); break;
    case 5 : result.append("five "); break;
    case 4 : result.append("four "); break;
    case 3 : result.append("three "); break;
    case 2 : result.append("two "); break;
    case 1 : result.append("one "); break;
    case 0 : result.append(""); break;
    }
  }
}
