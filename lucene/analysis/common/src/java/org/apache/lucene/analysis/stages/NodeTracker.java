package org.apache.lucene.analysis.stages;

/*
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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

// nocommit better name... NodeFreezer?

// nocommit can we absorb this into Stage ...? eg first
// Stage does this and all subseqeuent stages hold onto it?
public class NodeTracker {
  int nodeUpto;

  public int newNode() {
    return nodeUpto++;
  }

  public void reset() {
    nodeUpto = 0;
    numMayChangeNodes = 0;
  }

  /** Used by tests */
  boolean anyNodesCanChange() {
    return numMayChangeNodes != 0;
  }

  private int[] mayChangeNodes = new int[0];
  private int[] mayChangeRC = new int[0];
  private int numMayChangeNodes;

  /** A TokenFilter calls this to reserve the right to
   *  change a past node.  For every call to this method,
   *  that filter must later call wontChange to "free" the
   *  reservation.  There is no need to reserve either the
   *  from or the to node of the last token. */
  public void mayChange(int node) {
    // nocommit how to assert that node is "live"?  like you
    // can't suddenly un-freeze an already frozen node...
    for(int i=0;i<numMayChangeNodes;i++) {
      if (mayChangeNodes[i] == node) {
        mayChangeRC[i]++;
        return;
      }
    }

    if (numMayChangeNodes == mayChangeNodes.length) {
      mayChangeNodes = ArrayUtil.grow(mayChangeNodes, 1+numMayChangeNodes);
      mayChangeRC = ArrayUtil.grow(mayChangeRC, 1+numMayChangeNodes);
    }
    mayChangeNodes[numMayChangeNodes] = node;
    mayChangeRC[numMayChangeNodes] = 1;
    numMayChangeNodes++;
  }

  public void wontChange(int node) {
    for(int i=0;i<numMayChangeNodes;i++) {
      if (mayChangeNodes[i] == node) {
        mayChangeRC[i]--;
        if (mayChangeRC[i] == 0) {
          for(int j=i+1;j<numMayChangeNodes;j++) {
            mayChangeNodes[j-1] = mayChangeNodes[j];
            mayChangeRC[j-1] = mayChangeRC[j];
          }
          numMayChangeNodes--;
        }
        return;
      }
    }

    throw new IllegalStateException("extra call to wontChange(" + node + ") vs mayChange(" + node + ")");
  }

  public int getLastNode() {
    return nodeUpto-1;
  }

  public boolean getCanChange(int node) {
    for(int i=0;i<numMayChangeNodes;i++) {
      if (node == mayChangeNodes[i]) {
        return true;
      }
    }

    return false;
  }
}
