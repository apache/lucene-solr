package org.apache.lucene;

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

import java.util.GregorianCalendar;

import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.queryParser.*;

class SearchTest {
  public static void main(String[] args) {
    try {
      Directory directory = new RAMDirectory();  
      Analyzer analyzer = new SimpleAnalyzer();
      IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);

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
        d.add(new Field("contents", docs[j], Field.Store.YES, Field.Index.ANALYZED));
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

      QueryParser parser = new QueryParser("contents", analyzer);
      parser.setPhraseSlop(4);
      for (int j = 0; j < queries.length; j++) {
        Query query = parser.parse(queries[j]);
        System.out.println("Query: " + query.toString("contents"));

      //DateFilter filter =
      //  new DateFilter("modified", Time(1997,0,1), Time(1998,0,1));
      //DateFilter filter = DateFilter.Before("modified", Time(1997,00,01));
      //System.out.println(filter);

        ScoreDoc[] hits = searcher.search(query, null, docs.length).scoreDocs;

        System.out.println(hits.length + " total results");
        for (int i = 0 ; i < hits.length && i < 10; i++) {
          Document d = searcher.doc(hits[i].doc);
          System.out.println(i + " " + hits[i].score
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
