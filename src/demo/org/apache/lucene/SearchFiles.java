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
import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.lucene.analysis.Analyzer;
import com.lucene.analysis.StopAnalyzer;
import com.lucene.document.Document;
import com.lucene.search.Searcher;
import com.lucene.search.IndexSearcher;
import com.lucene.search.Query;
import com.lucene.search.Hits;
import com.lucene.queryParser.QueryParser;

class SearchFiles {
  public static void main(String[] args) {
    try {
      Searcher searcher = new IndexSearcher("index");
      Analyzer analyzer = new StopAnalyzer();

      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      while (true) {
	System.out.print("Query: ");
	String line = in.readLine();

	if (line.length() == -1)
	  break;

	Query query = QueryParser.parse(line, "contents", analyzer);
	System.out.println("Searching for: " + query.toString("contents"));

	Hits hits = searcher.search(query);
	System.out.println(hits.length() + " total matching documents");

	final int HITS_PER_PAGE = 10;
	for (int start = 0; start < hits.length(); start += HITS_PER_PAGE) {
	  int end = Math.min(hits.length(), start + HITS_PER_PAGE);
	  for (int i = start; i < end; i++)
	    System.out.println(i + ". " + hits.doc(i).get("path"));
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
