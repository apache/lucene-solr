package org.apache.lucenesandbox.xmlindexingdemo;

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
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Hits;
import org.apache.lucene.queryParser.QueryParser;

class SearchFiles {
  public static void main(String[] args) {
    try {
      Searcher searcher = new IndexSearcher("index");
      Analyzer analyzer = new StandardAnalyzer();

      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      while (true) {
	System.out.print("Query: ");
	String line = in.readLine();

	if (line.length() == -1)
	  break;

	Query query = QueryParser.parse(line, "name", analyzer);
	System.out.println("Searching for: " + query.toString("name"));

	Hits hits = searcher.search(query);
	System.out.println(hits.length() + " total matching documents");

	final int HITS_PER_PAGE = 10;
	for (int start = 0; start < hits.length(); start += HITS_PER_PAGE)
        {
	  int end = Math.min(hits.length(), start + HITS_PER_PAGE);
	  for (int i = start; i < end; i++)
          {
	    Document doc = hits.doc(i);
	    String name = doc.get("name");
	    System.out.println(name);
            System.out.println(doc.get("profession"));
            System.out.println(doc.get("addressLine1"));
            System.out.println(doc.get("addressLine2"));
            System.out.print(doc.get("city"));
            System.out.print(" ");
            System.out.print(doc.get("state"));
            System.out.print(" ");
            System.out.print(doc.get("zip"));
            System.out.println(doc.get("country"));

	  }

	  if (hits.length() > end) {
	    System.out.print("more (y/n) ? ");
	    line = in.readLine();
	    if (line.length() == 0 || line.charAt(0) == 'n')
	      break;
	  }
	}
      }
      searcher.close();

    } catch (Exception e) {
      System.out.println(" caught a " + e.getClass() +
			 "\n with message: " + e.getMessage());
    }
  }
}
