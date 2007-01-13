package org.apache.lucene.benchmark.byTask.feeds;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;

/**
 * Create sloppy phrase queries for performance test, in an index created using simple doc maker.
 */
public class SimpleSloppyPhraseQueryMaker extends SimpleQueryMaker {

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker#prepareQueries()
   */
  protected Query[] prepareQueries() throws Exception {
    // exatract some 100 words from doc text to an array
    String words[];
    ArrayList w = new ArrayList();
    StringTokenizer st = new StringTokenizer(SimpleDocMaker.DOC_TEXT);
    while (st.hasMoreTokens() && w.size()<100) {
      w.add(st.nextToken());
    }
    words = (String[]) w.toArray(new String[0]);

    // create queries (that would find stuff) with varying slops
    ArrayList queries = new ArrayList(); 
    for (int slop=0; slop<8; slop++) {
      for (int qlen=2; qlen<6; qlen++) {
        for (int wd=0; wd<words.length-qlen-slop; wd++) {
          // ordered
          int remainedSlop = slop;
          PhraseQuery q = new PhraseQuery();
          q.setSlop(slop);
          int wind = wd;
          for (int i=0; i<qlen; i++) {
            q.add(new Term(SimpleDocMaker.BODY_FIELD,words[wind++]));
            if (remainedSlop>0) {
              remainedSlop--;
              wind++;
            }
          }
          queries.add(q);
          // reveresed
          remainedSlop = slop;
          q = new PhraseQuery();
          q.setSlop(slop+2*qlen);
          wind = wd+qlen+remainedSlop-1;
          for (int i=0; i<qlen; i++) {
            q.add(new Term(SimpleDocMaker.BODY_FIELD,words[wind--]));
            if (remainedSlop>0) {
              remainedSlop--;
              wind--;
            }
          }
          queries.add(q);
        }
      }
    }
    return (Query[]) queries.toArray(new Query[0]);
  }

}
