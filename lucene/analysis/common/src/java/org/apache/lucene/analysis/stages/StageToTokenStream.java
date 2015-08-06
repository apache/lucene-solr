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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.stages.attributes.ArcAttribute;
import org.apache.lucene.analysis.stages.attributes.DeletedAttribute;
import org.apache.lucene.analysis.stages.attributes.OffsetAttribute;
import org.apache.lucene.analysis.stages.attributes.TermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO
//   - is there an adversary here?  that can cause
//     indefinite buffering?

// nocommit ToTokenizer instead? TokenFilter?

// nocommit make this more generic?  ie not just atts for
// current indexer ... eg use captureState/restoreState to
// pass through any custom atts too

/** This is a compatibility class, to map the new {@link Stage} API to
 *  the legacy {@link TokenStream} API currently used/required by
 *  consumers like {@link IndexWriter} and query parsers.  It takes
 *  a {@link Stage} as input and produces a {@link TokenStream} as
 *  output.  This is not general purpose: it currently only sets
 *  the attributes that the (core, no custom indexing chain) indexer 
 *  requires. */

public class StageToTokenStream extends TokenStream {

  // nocommit: cutover to the approach from SausageGraphFilter

  private final Stage stage;
  private final DeletedAttribute delAtt;
  private final TermAttribute termAttIn;
  private final ArcAttribute arcAttIn;
  private final OffsetAttribute offsetAttIn;

  private final org.apache.lucene.analysis.tokenattributes.CharTermAttribute termAttOut;
  private final org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute posIncAttOut;
  private final org.apache.lucene.analysis.tokenattributes.OffsetAttribute offsetAttOut;

  protected boolean resetCalled;
  protected boolean closeCalled = true;

  // Non-null when we are iterating through previously
  // buffered tokens:
  private Node[] pendingNodes;
  private int nodeUpto;
  private int arcUpto;
  private int lastPosition;
  private int pendingPosInc;

  private int finalEndOffset;

  // How many nodes in the current clump have no leaving arcs:
  private int frontierNodeCount;

  /** Holds a buffered node */
  private static class Node implements Comparable<Node> {
    int position;
    final List<Arc> leaving = new ArrayList<Arc>();

    public int compareTo(Node other) {
      // No tie break ... I think that's OK?
      return position - other.position;
    }
  }

  private static class Arc {
    final Node to;
    final String term;
    final int startOffset, endOffset;
    final boolean deleted;

    public Arc(Node to, String term, int startOffset, int endOffset, boolean deleted) {
      this.to = to;
      this.term = term;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
      this.deleted = deleted;
    }
  }

  public StageToTokenStream(Stage stage) {
    this.stage = stage;
    termAttIn = stage.get(TermAttribute.class);
    termAttOut = addAttribute(org.apache.lucene.analysis.tokenattributes.CharTermAttribute.class);
    posIncAttOut = addAttribute(org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute.class);
    offsetAttIn = stage.get(OffsetAttribute.class);
    offsetAttOut = addAttribute(org.apache.lucene.analysis.tokenattributes.OffsetAttribute.class);
    arcAttIn = stage.get(ArcAttribute.class);
    delAtt = stage.get(DeletedAttribute.class);
  }

  private Node getNode(Map<Integer,Node> nodes, int node) {
    Node n = nodes.get(node);
    if (n == null) {
      n = new Node();
      nodes.put(node, n);
      frontierNodeCount++;
    }
    return n;
  }

  private void saveToken(Map<Integer,Node> nodes) {
    Node from = nodes.get(arcAttIn.from());
    Node to = getNode(nodes, arcAttIn.to());
    to.position = Math.max(to.position, 1+from.position);
    if (from.leaving.isEmpty()) {
      frontierNodeCount--;
      assert frontierNodeCount >= 0;
    }
    from.leaving.add(new Arc(to, termAttIn.toString(), offsetAttIn.startOffset(), offsetAttIn.endOffset(), delAtt != null && delAtt.deleted()));
  }

  private boolean nextSavedToken() {
    while(pendingNodes != null) {
      // restore state from pending node/arc:
      Node node = pendingNodes[nodeUpto];
      if (node.leaving.isEmpty()) {
        assert nodeUpto == pendingNodes.length-1;
        pendingPosInc = node.position - lastPosition - 1;
        assert pendingPosInc >= 0;
        System.out.println("  break: posInc=" + pendingPosInc + " lastPos=" + lastPosition + " vs node.pos=" + node.position);
        break;
      }

      Arc arc = node.leaving.get(arcUpto);
      arcUpto++;
      if (arcUpto == node.leaving.size()) {
        nodeUpto++;
        if (nodeUpto == pendingNodes.length) {
          pendingPosInc = node.position - lastPosition;
          pendingNodes = null;
        } else {
          arcUpto = 0;
        }
      }

      if (!arc.deleted) {
        termAttOut.setEmpty();
        termAttOut.append(arc.term);
        offsetAttOut.setOffset(arc.startOffset, arc.endOffset);
        posIncAttOut.setPositionIncrement(node.position - lastPosition);
        System.out.println("    set posInc=" + (node.position - lastPosition));
        // TODO: it'd be trivial to also set PosLengthAtt
        // ... but since indexer is immediately after us, and
        // indexer ignores pos len, there's no point today
        //posLenAttOut.setPositionLength(arc.to.position - node.position);
        pendingPosInc = 0;
        lastPosition = node.position;
        System.out.println("  set lastPos=" + lastPosition);
        System.out.println("  return token=" + termAttOut);
        return true;
      } else {
        System.out.println("  skip deleted token");
      }
    }

    return false;
  }

  // nocommit this can falsely join two clumps into one, eg
  // two back-to-back synonyms

  @Override
  public final boolean incrementToken() throws IOException {
    System.out.println("STS.inc");

    if (resetCalled == false) {
      throw new IllegalStateException("call reset first");
    }

    // This is pointless (we always set all of the attrs we
    // export), but tests disagree:
    clearAttributes();

    if (pendingNodes != null) {
      // Still iterating through buffered tokens from last
      // clump:
      if (nextSavedToken()) {
        System.out.println("  buffered: " + termAttOut);
        return true;
      }
      System.out.println("  buffered fall through");
      // We can fall through to here, eg if the last
      // buffered token(s) were deleted (holes)
    }

    if (stage.next()) {
      if (stage.nodes.anyNodesCanChange()) {
        System.out.println("  now buffer: " + termAttIn);
        Map<Integer,Node> nodes = new HashMap<Integer,Node>();
        nodes.put(arcAttIn.from(), new Node());
        frontierNodeCount = 1;

        // Buffer up this "clump" of overlapping tokens
        // until it un-clumps itself:
        saveToken(nodes);
        while (true) {
          boolean result = stage.next();
          // So long as there are still nodes that can
          // change, there must be more tokens (hmm is this
          // really true...):
          assert result: "Stage.next ended without freezing all nodes";
          saveToken(nodes);
          System.out.println("  buffer again: " + termAttIn + "; " + stage.anyNodesCanChange() + " " + frontierNodeCount);
          if (!stage.anyNodesCanChange() && frontierNodeCount == 1) {
            System.out.println("  clump done");
            break;
          }
        }

        // Sort all nodes by position:
        pendingNodes = nodes.values().toArray(new Node[nodes.size()]);
        ArrayUtil.timSort(pendingNodes);
        for(Node node : pendingNodes) {
          System.out.println("  node pos=" + node.position + " " + node.leaving.size() + " leaving");
          for(Arc arc : node.leaving) {
            System.out.println("    " + arc.term + " to pos=" + arc.to.position);
          }
        }
        nodeUpto = 0;
        arcUpto = 0;
        lastPosition = -1;
        return nextSavedToken();

      } else {
        System.out.println("  pass through");
        // Fast path (pass through): no buffering necessary:
        termAttOut.setEmpty();
        termAttOut.append(termAttIn.get());
        offsetAttOut.setOffset(offsetAttIn.startOffset(),
                               offsetAttIn.endOffset());
        posIncAttOut.setPositionIncrement(1 + pendingPosInc);
        pendingPosInc = 0;
        return true;
      }
    } else {
      finalEndOffset = offsetAttIn.endOffset();
    }
    return false;
  }

  @Override
  public void end() throws IOException {
    super.end();
    offsetAttOut.setOffset(finalEndOffset, finalEndOffset);
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    pendingNodes = null;
    resetCalled = true;
  }

  @Override
  public void close() throws IOException {
    closeCalled = true;
  }
}
