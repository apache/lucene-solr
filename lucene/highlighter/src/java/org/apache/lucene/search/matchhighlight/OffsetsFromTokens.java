package org.apache.lucene.search.matchhighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This strategy works for fields where we know the match occurred but there are
 * no known positions or offsets.
 * <p>
 * We re-analyze field values and return offset ranges for returned tokens that
 * are also returned by the query's term collector.
 */
public final class OffsetsFromTokens implements OffsetsRetrievalStrategy {
  private final String field;
  private final Analyzer analyzer;

  public OffsetsFromTokens(String field, Analyzer analyzer) {
    this.field = field;
    this.analyzer = analyzer;
  }

  @Override
  public List<OffsetRange> get(MatchesIterator matchesIterator, MatchRegionRetriever.FieldValueProvider doc) throws IOException {
    List<CharSequence> values = doc.getValues(field);

    Set<BytesRef> matchTerms = new HashSet<>();
    while (matchesIterator.next()) {
      Query q = matchesIterator.getQuery();
      q.visit(new QueryVisitor() {
        @Override
        public void consumeTerms(Query query, Term... terms) {
          for (Term t : terms) {
            if (field.equals(t.field())) {
              matchTerms.add(t.bytes());
            }
          }
        }
      });
    }

    ArrayList<OffsetRange> ranges = new ArrayList<>();
    int valueOffset = 0;
    for (int valueIndex = 0, max = values.size(); valueIndex < max; valueIndex++) {
      final String value = values.get(valueIndex).toString();

      TokenStream ts = analyzer.tokenStream(field, value);
      OffsetAttribute offsetAttr = ts.getAttribute(OffsetAttribute.class);
      TermToBytesRefAttribute termAttr = ts.getAttribute(TermToBytesRefAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        if (matchTerms.contains(termAttr.getBytesRef())) {
          int startOffset = valueOffset + offsetAttr.startOffset();
          int endOffset = valueOffset + offsetAttr.endOffset();
          ranges.add(new OffsetRange(startOffset, endOffset));
        }
      }
      ts.end();
      valueOffset += offsetAttr.endOffset() + analyzer.getOffsetGap(field);
      ts.close();
    }
    return ranges;
  }

  @Override
  public boolean requiresDocument() {
    return true;
  }
}
