package org.apache.lucene.search.poshighlight;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

public class PosHighlighter {
  private Formatter formatter;
  private Encoder encoder;


  public PosHighlighter() {
      this(new SimpleHTMLFormatter());
  }
  
  public PosHighlighter(Formatter formatter) {
      this(formatter,new DefaultEncoder());
  }

  public PosHighlighter(Formatter formatter, Encoder encoder) {
      this.formatter = formatter;
      this.encoder = encoder;
  }

  /**
   * 
   * @param scorer a Scorer positioned at the docID for which highlighting
   * fragments are to be retrieved.
   * @param mergeContiguousFragments
   * @param maxNumFragments the number of fragments to return
   * @param fragSize, the requested size of fragments, in characters.  Fragments may 
   * be smaller if there is insufficient text.  There is an qttempt to put the first match 
   * in the center of the fragment.
   * @return the first maxNumFragments TextFragments, ordered by (descending) score.  
   * Each fragment corresponds to a Span, and its score is the Span's score.
   * @throws IOException 
   */  
  public String[] getFirstFragments(
      ScorePosDoc doc,
      IndexReader reader,
      String fieldName,
      boolean mergeContiguousFragments,
      int maxNumFragments,
      int fragSize) throws IOException
  {
    PositionOffsetMapper pom = new PositionOffsetMapper ();
    // FIXME: test error cases: for non-stored fields, and fields w/no term vectors
    reader.getTermFreqVector(doc.doc, fieldName, pom);
    String text = reader.document(doc.doc).getFieldable(fieldName).stringValue();    
    String[] frags = new String[maxNumFragments];
    int ifrag = 0;
    int ipos = 0;
    PositionInterval pos = doc.positions[ipos++];
    while (ifrag < maxNumFragments && pos != null) {
      StringBuilder buf = new StringBuilder();
      int matchStart = pom.getStartOffset(pos.begin);
      int matchEnd = pom.getEndOffset(pos.end);
      int fragStart = Math.max(0, matchStart - (fragSize - (matchEnd-matchStart)) / 2);
      int fragEnd = Math.min(fragStart+fragSize, text.length());
      
      for (;;) {
        // Build up a single fragment, possibly including multiple positions
        if (matchStart > fragStart)
          buf.append (text, fragStart, matchStart);
        buf.append ("<em>"); // TODO - parameterize
        buf.append (text, matchStart, matchEnd);
        buf.append ("</em>");
        if (fragEnd <= matchEnd) {
          break;
        }
        boolean done = false;
        if (ipos < doc.posCount) {
          pos = doc.positions[ipos++];
          matchStart = pom.getStartOffset(pos.begin);
          done = (matchStart >= fragEnd);
        }
        else {
          pos = null; // terminate the outer loop
          done = true;
        }
        if (done) {
          // Either there are no more matches or the next match comes after the end of this fragment
          // In either case, grab some more text to fill out the fragment
          buf.append(text, matchEnd, fragEnd);
          break;
        }
        // include the next match in this fragment
        fragStart = matchEnd;
        matchEnd = pom.getEndOffset(pos.end);
      }
      // emit a completed fragment
      frags[ifrag++] = buf.toString();
    }
    return frags;
  }

  /**
   * @param scorer
   * @param mergeContiguousFragments
   * @param maxNumFragments number of fragments to retrieve
   * @return The first maxNumFragments TextFragments, in document order: 
   * sorted by their (start ing, then ending) span position 
   */
  public TextFragment[] getBestFragments(
      ScorePosDoc doc,
      IndexReader reader,
      boolean mergeContiguousFragments,
      int maxNumFragments)
  {
    // TODO - get maxNumFragments top fragments by score
    return null;
  }
  
  class PositionOffsetMapper extends TermVectorMapper {
    private int maxPos = 0;
    private static final int BUF_SIZE = 128;
    int startOffset[] = new int[BUF_SIZE], endOffset[] = new int[BUF_SIZE];   

    public void setExpectations(String field, int numTerms,
        boolean storeOffsets, boolean storePositions) {
    }

    public void map(BytesRef term, int frequency,
        TermVectorOffsetInfo[] offsets, int[] positions) 
    {
      for (int i = 0; i < positions.length; i++) {
        int pos = positions[i];
        if (pos >= startOffset.length) {
          grow (pos + BUF_SIZE);
          maxPos = pos;
        } else if (pos > maxPos) {
          maxPos = pos;
        }
        startOffset[pos] = offsets[i].getStartOffset();
        endOffset[pos] = offsets[i].getEndOffset();
      }
    }
    
    private void grow (int size) {
      startOffset = ArrayUtil.grow (startOffset, size);
      endOffset = ArrayUtil.grow (endOffset, size);
    }
    
    public int getStartOffset(int pos) {
      return startOffset[pos];
    }
    
    public int getEndOffset(int pos) {
      return endOffset[pos];
    }
    
    public int getMaxPosition() {
      return maxPos;
    }
  }
}
