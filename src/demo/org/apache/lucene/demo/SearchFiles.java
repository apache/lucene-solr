package org.apache.lucene.demo;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

	Query query = QueryParser.parse(line, "contents", analyzer);
	System.out.println("Searching for: " + query.toString("contents"));

	Hits hits = searcher.search(query);
	System.out.println(hits.length() + " total matching documents");

	final int HITS_PER_PAGE = 10;
	for (int start = 0; start < hits.length(); start += HITS_PER_PAGE) {
	  int end = Math.min(hits.length(), start + HITS_PER_PAGE);
	  for (int i = start; i < end; i++) {
	    Document doc = hits.doc(i);
	    String path = doc.get("path");
	    if (path != null) {
              System.out.println(i + ". " + path);
	    } else {
              String url = doc.get("url");
	      if (url != null) {
		System.out.println(i + ". " + url);
		System.out.println("   - " + doc.get("title"));
	      } else {
		System.out.println(i + ". " + "No path nor URL for this document");
	      }
	    }
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
