package org.apache.lucene.search.poshighlight;

import java.io.IOException;

import org.apache.lucene.search.positions.PositionIntervalIterator;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;

// retrieves the positions from the leaves of a tree of PositionIntervalIterators
public class PositionTreeIterator {
    
    static class Frame {
      Frame (PositionIntervalIterator positions) {
        this.positions = positions;
        subs = positions.subs(true);
        if (subs.length == 0)
          subs = null;
        isub = (subs != null) ? 0 : -1;
      }
      PositionIntervalIterator positions;
      PositionIntervalIterator subs[];
      int isub;
    };
    
    Frame stack[] = new Frame[32];
    int curframe = 0;
    
    public PositionTreeIterator (PositionIntervalIterator root) {
      stack[0] = new Frame(root);
    }
    
    public PositionInterval next() throws IOException {
      PositionInterval pos;
      if (curframe < 0)
        return null;
      Frame f = stack[curframe];      
      if (f.subs == null) {
        pos = stack[curframe].positions.next();
        if (pos != null)
          return pos;
      }
      else if (f.isub < f.subs.length) {
        if (curframe >= stack.length) {
          throw new ArrayIndexOutOfBoundsException ("PositionTreeIterator stack depth > 32");
        }
        stack[++curframe] = new Frame (f.subs[f.isub++]);
        return next();
      }
      // pop
      --curframe;
      return next();
    }
  }
