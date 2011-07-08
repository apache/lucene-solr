package org.apache.lucene.search.poshighlight;

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.positions.PositionIntervalIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.util.ArrayUtil;

/** Used to accumulate span positions while scoring */
public class ScorePosDoc extends ScoreDoc {
  
  public int posCount = 0;
  public PositionInterval[] positions;
  
  public ScorePosDoc(int doc, float score, PositionIntervalIterator posIter, int maxPositions, boolean orderByScore) throws IOException
  {
    super(doc, score);
    assert doc == posIter.docID();
    positions = new PositionInterval[32];
    storePositions (new PositionTreeIterator (posIter), maxPositions, orderByScore);
  }
  
  private void storePositions(PositionTreeIterator ptree,
      int maxPositions, boolean orderByScore) throws IOException {

    for (PositionInterval pos = ptree.next(); pos != null; pos = ptree.next()) {
      if (posCount >= positions.length) {
        PositionInterval temp[] = new PositionInterval[positions.length * 2];
        System.arraycopy(positions, 0, temp, 0, positions.length);
      }
      positions[posCount++] = (PositionInterval) pos.clone();
    }
    ArrayUtil.mergeSort(positions, 0, posCount, new Comparator<PositionInterval>() {
      public int compare(PositionInterval o1, PositionInterval o2) {
        return 
          o1.begin < o2.begin ? -1 : 
            (o1.begin > o2.begin ? 1 :
              (o1.end < o2.end ? -1 : 
                (o1.end > o2.end ? 1 : 
                  0)));
      }
      
    });
  }

}
