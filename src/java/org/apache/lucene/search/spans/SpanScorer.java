package org.apache.lucene.search.spans;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Similarity;


class SpanScorer extends Scorer {
  private Spans spans;
  private Weight weight;
  private byte[] norms;
  private float value;

  private boolean firstTime = true;
  private boolean more = true;

  private int doc;
  private float freq;

  SpanScorer(Spans spans, Weight weight, Similarity similarity, byte[] norms)
    throws IOException {
    super(similarity);
    this.spans = spans;
    this.norms = norms;
    this.weight = weight;
    this.value = weight.getValue();
  }

  public boolean next() throws IOException {
    if (firstTime) {
      more = spans.next();
      firstTime = false;
    }

    if (!more) return false;

    freq = 0.0f;
    doc = spans.doc();

    while (more && doc == spans.doc()) {
      int matchLength = spans.end() - spans.start();
      freq += getSimilarity().sloppyFreq(matchLength);
      more = spans.next();
    }

    return more || freq != 0.0f;
  }

  public int doc() { return doc; }

  public float score() throws IOException {
    float raw = getSimilarity().tf(freq) * value; // raw score
    return raw * Similarity.decodeNorm(norms[doc]); // normalize
  }

  public boolean skipTo(int target) throws IOException {
    more = spans.skipTo(target);

    if (!more) return false;

    freq = 0.0f;
    doc = spans.doc();

    while (more && spans.doc() == target) {
      freq += getSimilarity().sloppyFreq(spans.end() - spans.start());
      more = spans.next();
    }

    return more || freq != 0.0f;
  }

  public Explanation explain(final int doc) throws IOException {
    Explanation tfExplanation = new Explanation();

    skipTo(doc);

    float phraseFreq = (doc() == doc) ? freq : 0.0f;
    tfExplanation.setValue(getSimilarity().tf(phraseFreq));
    tfExplanation.setDescription("tf(phraseFreq=" + phraseFreq + ")");

    return tfExplanation;
  }

}
