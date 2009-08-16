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
package org.apache.solr.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.*;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TextField;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;


/**
 * Create a field query from the input value, applying text analysis and constructing a phrase query if appropriate.
 * <br>Other parameters: <code>f</code>, the field
 * <br>Example: <code>{!field f=myfield}Foo Bar</code> creates a phrase query with "foo" followed by "bar"
 * if the analyzer for myfield is a text field with an analyzer that splits on whitespace and lowercases terms.
 * This is generally equivalent to the Lucene query parser expression <code>myfield:"Foo Bar"</code>
 */
public class FieldQParserPlugin extends QParserPlugin {
  public static String NAME = "field";

  public void init(NamedList args) {
  }

  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      public Query parse() throws ParseException {
        String field = localParams.get(QueryParsing.F);
        String queryText = localParams.get(QueryParsing.V);
        FieldType ft = req.getSchema().getFieldType(field);
        if (!(ft instanceof TextField)) {
          String internal = ft.toInternal(queryText);
          return new TermQuery(new Term(field, internal));
        }

        int phraseSlop = 0;
        Analyzer analyzer = req.getSchema().getQueryAnalyzer();

        // most of the following code is taken from the Lucene QueryParser

        // Use the analyzer to get all the tokens, and then build a TermQuery,
        // PhraseQuery, or nothing based on the term count

        TokenStream source = null;
        try {
          source = analyzer.reusableTokenStream(field, new StringReader(queryText));
          source.reset();
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);  
        }
        ArrayList<Token> lst = new ArrayList<Token>();
        Token t;
        int positionCount = 0;
        boolean severalTokensAtSamePosition = false;

        while (true) {
          try {
            t = source.next();
          }
          catch (IOException e) {
            t = null;
          }
          if (t == null)
            break;
          lst.add(t);
          if (t.getPositionIncrement() != 0)
            positionCount += t.getPositionIncrement();
          else
            severalTokensAtSamePosition = true;
        }
        try {
          source.close();
        }
        catch (IOException e) {
          // ignore
        }

        if (lst.size() == 0)
          return null;
        else if (lst.size() == 1) {
          t = lst.get(0);
          return new TermQuery(new Term(field, new String(t.termBuffer(), 0, t.termLength())));
        } else {
          if (severalTokensAtSamePosition) {
            if (positionCount == 1) {
              // no phrase query:
              BooleanQuery q = new BooleanQuery(true);
              for (int i = 0; i < lst.size(); i++) {
                t = (org.apache.lucene.analysis.Token) lst.get(i);
                TermQuery currentQuery = new TermQuery(
                        new Term(field, new String(t.termBuffer(), 0, t.termLength())));
                q.add(currentQuery, BooleanClause.Occur.SHOULD);
              }
              return q;
            }
            else {
              // phrase query:
              MultiPhraseQuery mpq = new MultiPhraseQuery();
              mpq.setSlop(phraseSlop);
              ArrayList multiTerms = new ArrayList();
              for (int i = 0; i < lst.size(); i++) {
                t = (org.apache.lucene.analysis.Token) lst.get(i);
                if (t.getPositionIncrement() == 1 && multiTerms.size() > 0) {
                  mpq.add((Term[])multiTerms.toArray(new Term[0]));
                  multiTerms.clear();
                }
                multiTerms.add(new Term(field, new String(t.termBuffer(), 0, t.termLength())));
              }
              mpq.add((Term[])multiTerms.toArray(new Term[0]));
              return mpq;
            }
          }
          else {
            PhraseQuery q = new PhraseQuery();
            q.setSlop(phraseSlop);
            for (int i = 0; i < lst.size(); i++) {
              Token token = lst.get(i);
              q.add(new Term(field, new String(token.termBuffer(), 0, token.termLength())));
            }
            return q;
          }
        }
      }
    };
  }
}
