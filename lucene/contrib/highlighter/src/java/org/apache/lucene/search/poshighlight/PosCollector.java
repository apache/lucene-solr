package org.apache.lucene.search.poshighlight;

import java.io.IOException;

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Scorer.ScorerVisitor;

/**
 * for testing only - collect the first maxDocs docs and throw the rest away
 */
public class PosCollector extends Collector {
  
  int count;
  ScorePosDoc docs[];
  
  public PosCollector (int maxDocs) {
    docs = new ScorePosDoc[maxDocs];
  }
  
  protected Scorer scorer;
  
  public void collect(int doc) throws IOException {
    if (count >= docs.length)
      return;
    assert (scorer != null);
    ScorePosDoc spdoc = new ScorePosDoc (doc, scorer.score(), scorer.positions(), 32, false);
    docs[count++] = spdoc;
  }

  public boolean acceptsDocsOutOfOrder() {
    return false;
  }

  public void setScorer(Scorer scorer) {
    this.scorer = scorer;
  }
  
  public Scorer getScorer () {
    return scorer;
  }
  
  public ScorePosDoc[] getDocs () {
    ScorePosDoc ret[] = new ScorePosDoc[count];
    System.arraycopy(docs, 0, ret, 0, count);
    return ret;
  }

  public void setNextReader(AtomicReaderContext context) throws IOException {
  }
  
  @Override
  public boolean needsPositions() { return true; }

  /**
   * For testing/investigation
   * @author sokolov
   *
   */
  class SpanScorerVisitor extends ScorerVisitor<Query, Query, Scorer> {
    
    @Override 
    public void visitRequired (Query parent, Query child, Scorer scorer) {
      System.out.println ("parent=" + parent + ", child=" + child);
    }
    
    @Override 
    public void visitOptional (Query parent, Query child, Scorer scorer) {
      System.out.println ("parent=" + parent + ", child=" + child);
    }
  }
}
