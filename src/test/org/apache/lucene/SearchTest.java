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

import java.io.IOException;
import java.util.Date;
import java.util.GregorianCalendar;

import com.lucene.store.*;
import com.lucene.document.*;
import com.lucene.analysis.*;
import com.lucene.index.*;
import com.lucene.search.*;
import com.lucene.queryParser.*;

class SearchTest {
  public static void main(String[] args) {
    try {
      Directory directory = new RAMDirectory();  
      Analyzer analyzer = new SimpleAnalyzer();
      IndexWriter writer = new IndexWriter(directory, analyzer, true);

      String[] docs = {
	"a b c d e",
	"a b c d e a b c d e",
	"a b c d e f g h i j",
	"a c e",
	"e c a",
	"a c e a c e",
	"a c e a b c"
      };
      for (int j = 0; j < docs.length; j++) {
	Document d = new Document();
	d.add(Field.Text("contents", docs[j]));
	writer.addDocument(d);
      }
      writer.close();

      Searcher searcher = new IndexSearcher(directory);
      
      String[] queries = {
// 	"a b",
// 	"\"a b\"",
// 	"\"a b c\"",
// 	"a c",
// 	"\"a c\"",
	"\"a c e\"",
      };
      Hits hits = null;

      QueryParser parser = new QueryParser("contents", analyzer);
      parser.setPhraseSlop(4);
      for (int j = 0; j < queries.length; j++) {
	Query query = parser.parse(queries[j]);
	System.out.println("Query: " + query.toString("contents"));

      //DateFilter filter =
      //  new DateFilter("modified", Time(1997,0,1), Time(1998,0,1));
      //DateFilter filter = DateFilter.Before("modified", Time(1997,00,01));
      //System.out.println(filter);

	hits = searcher.search(query, null);

	System.out.println(hits.length() + " total results");
	for (int i = 0 ; i < hits.length() && i < 10; i++) {
	  Document d = hits.doc(i);
	  System.out.println(i + " " + hits.score(i)
// 			   + " " + DateField.stringToDate(d.get("modified"))
			     + " " + d.get("contents"));
	}
      }
      searcher.close();
      
    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
  }

  static long Time(int year, int month, int day) {
    GregorianCalendar calendar = new GregorianCalendar();
    calendar.set(year, month, day);
    return calendar.getTime().getTime();
  }
}
