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

package org.apache.lucene.analysis.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.RollingBuffer;

/**
 * Converts an incoming graph token stream, such as one from
 * {@link SynonymGraphFilter}, into a flat form so that
 * all nodes form a single linear chain with no side paths.  Every
 * path through the graph touches every node.  This is necessary
 * when indexing a graph token stream, because the index does not
 * save {@link PositionLengthAttribute} and so it cannot
 * preserve the graph structure.  However, at search time,
 * query parsers can correctly handle the graph and this token
 * filter should <b>not</b> be used.
 *
 * <p>If the graph was not already flat to start, this
 * is likely a lossy process, i.e. it will often cause the 
 * graph to accept token sequences it should not, and to
 * reject token sequences it should not.
 *
 * <p>However, when applying synonyms during indexing, this
 * is necessary because Lucene already does not index a graph 
 * and so the indexing process is already lossy
 * (it ignores the {@link PositionLengthAttribute}).
 *
 * @lucene.experimental
 */
public final class FlattenGraphFilter extends TokenFilter {

  /** Holds all tokens leaving a given input position. */
  private final static class InputNode implements RollingBuffer.Resettable {
    private final List<AttributeSource.State> tokens = new ArrayList<>();

    /** Our input node, or -1 if we haven't been assigned yet */
    int node = -1;

    /** Maximum to input node for all tokens leaving here; we use this
     *  to know when we can freeze. */
    int maxToNode = -1;

    /** Minimum to input node for all tokens leaving here; we use this to check if holes exist. */
    int minToNode = Integer.MAX_VALUE;

    /**
     * Where we currently map to; this changes (can only increase as we see more input tokens),
     * until we are finished with this position.
     */
    int outputNode = -1;

    /** Which token (index into {@link #tokens}) we will next output. */
    int nextOut;

    @Override
    public void reset() {
      tokens.clear();
      node = -1;
      outputNode = -1;
      maxToNode = -1;
      minToNode = Integer.MAX_VALUE;
      nextOut = 0;
    }
  }

  /** Gathers up merged input positions into a single output position,
   *  only for the current "frontier" of nodes we've seen but can't yet
   *  output because they are not frozen. */
  private final static class OutputNode implements RollingBuffer.Resettable {
    private final List<Integer> inputNodes = new ArrayList<>();

    /** Node ID for this output, or -1 if we haven't been assigned yet. */
    int node = -1;

    /** Which input node (index into {@link #inputNodes}) we will next output. */
    int nextOut;
    
    /** Start offset of tokens leaving this node. */
    int startOffset = -1;

    /** End offset of tokens arriving to this node. */
    int endOffset = -1;

    @Override
    public void reset() {
      inputNodes.clear();
      node = -1;
      nextOut = 0;
      startOffset = -1;
      endOffset = -1;
    }
  }

  private final RollingBuffer<InputNode> inputNodes = new RollingBuffer<InputNode>() {
    @Override
    protected InputNode newInstance() {
      return new InputNode();
    }
  };

  private final RollingBuffer<OutputNode> outputNodes = new RollingBuffer<OutputNode>() {
    @Override
    protected OutputNode newInstance() {
      return new OutputNode();
    }
  };

  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  /** Which input node the last seen token leaves from */
  private int inputFrom;

  /** We are currently releasing tokens leaving from this output node */
  private int outputFrom;

  // for debugging:
  //private int retOutputFrom;

  private boolean done;

  private int lastOutputFrom;

  private int finalOffset;

  private int finalPosInc;

  private int maxLookaheadUsed;

  private int lastStartOffset;

  public FlattenGraphFilter(TokenStream in) {
    super(in);
  }

  private boolean releaseBufferedToken() {

    // We only need the while loop (retry) if we have a hole (an output node that has no tokens leaving):
    while (outputFrom < outputNodes.getMaxPos()) {
      OutputNode output = outputNodes.get(outputFrom);
      if (output.inputNodes.isEmpty()) {
        // No tokens arrived to this node, which happens for the first node
        // after a hole:
        //System.out.println("    skip empty outputFrom=" + outputFrom);
        outputFrom++;
        continue;
      }

      int maxToNode = -1;
      for(int inputNodeID : output.inputNodes) {
        InputNode inputNode = inputNodes.get(inputNodeID);
        assert inputNode.outputNode == outputFrom;
        maxToNode = Math.max(maxToNode, inputNode.maxToNode);
      }
      //System.out.println("  release maxToNode=" + maxToNode + " vs inputFrom=" + inputFrom);

      // TODO: we could shrink the frontier here somewhat if we
      // always output posLen=1 as part of our "sausagizing":
      if (maxToNode <= inputFrom || done) {
        //System.out.println("  output node merged these inputs: " + output.inputNodes);
        // These tokens are now frozen
        assert output.nextOut < output.inputNodes.size(): "output.nextOut=" + output.nextOut + " vs output.inputNodes.size()=" + output.inputNodes.size();
        InputNode inputNode = inputNodes.get(output.inputNodes.get(output.nextOut));
        if (done && inputNode.tokens.size() == 0 && outputFrom >= outputNodes.getMaxPos()) {
          return false;
        }
        if (inputNode.tokens.size() == 0) {
          assert inputNode.nextOut == 0;
          // Hole dest nodes should never be merged since 1) we always
          // assign them to a new output position, and 2) since they never
          // have arriving tokens they cannot be pushed. Skip them but don't free
          // input until all are checked.
          // Related tests testAltPathLastStepLongHole, testAltPathLastStepHoleFollowedByHole,
          // testAltPathLastStepHoleWithoutEndToken
          if (output.inputNodes.size() > 1) {
            output.nextOut++;
            if (output.nextOut < output.inputNodes.size()) {
              continue;
            }
          }
          // Don't free from a hole src. Since no edge leaves here book keeping may be incorrect.
          // Later output nodes may point to earlier input nodes. So we don't want to free them yet.
          freeBefore(output);
          continue;
        }

        assert inputNode.nextOut < inputNode.tokens.size();

        restoreState(inputNode.tokens.get(inputNode.nextOut));

        // Correct posInc
        assert outputFrom >= lastOutputFrom;
        posIncAtt.setPositionIncrement(outputFrom - lastOutputFrom);
        int toInputNodeID = inputNode.node + posLenAtt.getPositionLength();
        InputNode toInputNode = inputNodes.get(toInputNodeID);

        // Correct posLen
        assert toInputNode.outputNode > outputFrom;
        posLenAtt.setPositionLength(toInputNode.outputNode - outputFrom);
        lastOutputFrom = outputFrom;
        inputNode.nextOut++;
        //System.out.println("  ret " + this);

        OutputNode outputEndNode = outputNodes.get(toInputNode.outputNode);

        // Correct offsets

        // This is a bit messy; we must do this so offset don't go backwards,
        // which would otherwise happen if the replacement has more tokens
        // than the input:
        int startOffset = Math.max(lastStartOffset, output.startOffset);

        // We must do this in case the incoming tokens have broken offsets:
        int endOffset = Math.max(startOffset, outputEndNode.endOffset);
        
        offsetAtt.setOffset(startOffset, endOffset);
        lastStartOffset = startOffset;

        if (inputNode.nextOut == inputNode.tokens.size()) {
          output.nextOut++;
          if (output.nextOut == output.inputNodes.size()) {
            freeBefore(output);
          }
        }

        return true;
      } else {
        return false;
      }
    }

    //System.out.println("    break false");
    return false;
  }

  /**
   * Free inputs nodes before the minimum input node for the given output.
   *
   * @param output target output node
   */
  private void freeBefore(OutputNode output) {
    /* We've released all of the tokens that end at the current output, so free all output nodes before this.
    Input nodes are more complex. The second shingled tokens with alternate paths can appear later in the output graph
    than some of their alternate path tokens. Because of this case we can only free from the minimum because
    the minimum node will have come from before the second shingled token.
    This means we have to hold onto input nodes whose tokens get stacked on previous nodes until
    we've completely passed those inputs.
    Related tests testShingledGap, testShingledGapWithHoles
    */
    outputFrom++;
    int freeBefore = Collections.min(output.inputNodes);
    // This will catch a node being freed early if it is input to the next output.
    // Could a freed early node be input to a later output?
    assert outputNodes.get(outputFrom).inputNodes.stream().filter(n -> freeBefore > n).count() == 0
        : "FreeBefore " + freeBefore + " will free in use nodes";
    inputNodes.freeBefore(freeBefore);
    outputNodes.freeBefore(outputFrom);
  }

  @Override
  public boolean incrementToken() throws IOException {
    //System.out.println("\nF.increment inputFrom=" + inputFrom + " outputFrom=" + outputFrom);

    while (true) {
      if (releaseBufferedToken()) {
        //retOutputFrom += posIncAtt.getPositionIncrement();
        //System.out.println("    return buffered: " + termAtt + " " + retOutputFrom + "-" + (retOutputFrom + posLenAtt.getPositionLength()));
        //printStates();
        return true;
      } else if (done) {
        //System.out.println("    done, return false");
        return false;
      }

      if (input.incrementToken()) {
        // Input node this token leaves from:
        int positionIncrement = posIncAtt.getPositionIncrement();
        inputFrom += positionIncrement;

        int startOffset = offsetAtt.startOffset();
        int endOffset = offsetAtt.endOffset();

        // Input node this token goes to:
        int inputTo = inputFrom + posLenAtt.getPositionLength();
        //System.out.println("  input.inc " + termAtt + ": " + inputFrom + "-" + inputTo);

        InputNode src = inputNodes.get(inputFrom);
        if (src.node == -1) {
          recoverFromHole(src, startOffset, positionIncrement);

        } else {
          OutputNode outSrc = outputNodes.get(src.outputNode);
          /* If positionIncrement > 1 and the position we're incrementing from doesn't come to the current node we've crossed a hole.
           * The long edge will point too far back and not account for the holes unless it gets fixed.
           * example:
           *  _____abc______
           * |              |
           * |              V
           * O-a->O- ->O- ->O-d->O
           *
           * A long edge may have already made this fix though, if src is more than 1 position ahead in the output there's no additional work to do
           * example:
           *  _____abc______
           * |    ....bc....|
           * |    .        VV
           * O-a->O- ->O- ->O-d->O
           */
          if (positionIncrement > 1
              && src.outputNode - inputNodes.get(inputFrom - positionIncrement).outputNode <= 1
              && inputNodes.get(inputFrom - positionIncrement).minToNode != inputFrom) {
            /* If there was a hole at the end of an alternate path then the input and output nodes
             * have been created,
             * but the offsets and increments have not been maintained correctly. Here we go back
             * and fix them.
             * Related test testAltPathLastStepHole
             * The last node in the alt path didn't arrive to remove this reference.
             */
            assert inputNodes.get(inputFrom).tokens.isEmpty() : "about to remove non empty edge";
            outSrc.inputNodes.remove(Integer.valueOf(inputFrom));
            src.outputNode = -1;
            int prevEndOffset = outSrc.endOffset;

            outSrc = recoverFromHole(src, startOffset, positionIncrement);
            outSrc.endOffset = prevEndOffset;
          }

          if (outSrc.startOffset == -1 || startOffset > outSrc.startOffset) {
            // "shrink wrap" the offsets so the original tokens (with most
            // restrictive offsets) win:
            outSrc.startOffset = Math.max(startOffset, outSrc.startOffset);
          }
        }

        // Buffer this token:
        src.tokens.add(captureState());
        src.maxToNode = Math.max(src.maxToNode, inputTo);
        src.minToNode = Math.min(src.minToNode, inputTo);
        maxLookaheadUsed = Math.max(maxLookaheadUsed, inputNodes.getBufferSize());

        InputNode dest = inputNodes.get(inputTo);
        if (dest.node == -1) {
          // Common case: first time a token is arriving to this input position:
          dest.node = inputTo;
        }

        // Always number output nodes sequentially:
        int outputEndNode = src.outputNode + 1;

        if (outputEndNode > dest.outputNode) {
          if (dest.outputNode != -1) {
            boolean removed = outputNodes.get(dest.outputNode).inputNodes.remove(Integer.valueOf(inputTo));
            assert removed;
          }
          //System.out.println("    increase output node: " + dest.outputNode + " vs " + outputEndNode);
          outputNodes.get(outputEndNode).inputNodes.add(inputTo);
          dest.outputNode = outputEndNode;

          // Since all we ever do is merge incoming nodes together, and then renumber
          // the merged nodes sequentially, we should only ever assign smaller node
          // numbers:
          assert outputEndNode <= inputTo: "outputEndNode=" + outputEndNode + " vs inputTo=" + inputTo;
        }

        OutputNode outDest = outputNodes.get(dest.outputNode);
        // "shrink wrap" the offsets so the original tokens (with most
        // restrictive offsets) win:
        if (outDest.endOffset == -1 || endOffset < outDest.endOffset) {
          outDest.endOffset = endOffset;
        }

      } else {
        //System.out.println("  got false from input");
        input.end();
        finalPosInc = posIncAtt.getPositionIncrement();
        finalOffset = offsetAtt.endOffset();
        done = true;
        // Don't return false here: we need to force release any buffered tokens now
      }
    }
  }

  private OutputNode recoverFromHole(InputNode src, int startOffset, int posinc) {
    // This means the "from" node of this token was never seen as a "to" node,
    // which should only happen if we just crossed a hole.  This is a challenging
    // case for us because we normally rely on the full dependencies expressed
    // by the arcs to assign outgoing node IDs.  It would be better if tokens
    // were never dropped but instead just marked deleted with a new
    // TermDeletedAttribute (boolean valued) ... but until that future, we have
    // a hack here to forcefully jump the output node ID:
    assert src.outputNode == -1;
    src.node = inputFrom;

    int outIndex;
    int previousInputFrom = inputFrom - posinc;
    if (previousInputFrom >= 0) {
      InputNode offsetSrc = inputNodes.get(previousInputFrom);
      /* Select output src node. Need to make sure the new output node isn't placed too far ahead.
       * If a disconnected node is placed at the end of the output graph that may place it after output nodes that map to input nodes that are after src in the input.
       * Since it is disconnected there is no path to it, and there could be holes after meaning no paths to following nodes. This "floating" edge will cause problems in FreeBefore.
       * In the following section make sure the edge connects to something.
       * Related test testLongHole testAltPathLastStepHoleFollowedByHole, testAltPathFirstStepHole, testShingledGapWithHoles
       */
      if (offsetSrc.minToNode < inputFrom) {
        // There is a possible path to this node.
        // place this node one position off from the possible path keeping a 1 inc gap.
        // Can't be larger than 1 inc or risk getting disconnected.
        outIndex = inputNodes.get(offsetSrc.minToNode).outputNode + 1;
      } else {
        // no information about how the current node was previously connected.
        // Connect it to the end.
        outIndex = outputNodes.getMaxPos();
      }
    } else {
      // in case the first token in the stream is a hole we have no input node to increment from.
      outIndex = outputNodes.getMaxPos() + 1;
    }
    OutputNode outSrc = outputNodes.get(outIndex);
    src.outputNode = outIndex;

    // OutSrc may have other inputs
    if (outSrc.node == -1) {
      outSrc.node = src.outputNode;
      outSrc.startOffset = startOffset;
    } else {
      outSrc.startOffset = Math.max(startOffset, outSrc.startOffset);
    }
    outSrc.inputNodes.add(inputFrom);
    return outSrc;
  }

  // Only for debugging:
  /*
  private void printStates() {
    System.out.println("states:");
    for(int i=outputFrom;i<outputNodes.getMaxPos();i++) {
      OutputNode outputNode = outputNodes.get(i);
      System.out.println("  output " + i + ": inputs " + outputNode.inputNodes);
      for(int inputNodeID : outputNode.inputNodes) {
        InputNode inputNode = inputNodes.get(inputNodeID);
        assert inputNode.outputNode == i;
      }
    }
  }
  */

  @Override
  public void end() throws IOException {
    if (done == false) {
      super.end();
    } else {
      // NOTE, shady: don't call super.end, because we did already from incrementToken
    }

    clearAttributes();
    if (done) {
      // On exc, done is false, and we will not have set these:
      posIncAtt.setPositionIncrement(finalPosInc);
      offsetAtt.setOffset(finalOffset, finalOffset);
    } else {
      super.end();
    }
  }
  
  @Override
  public void reset() throws IOException {
    //System.out.println("F: reset");
    super.reset();
    inputFrom = -1;
    inputNodes.reset();
    InputNode in = inputNodes.get(0);
    in.node = 0;
    in.outputNode = 0;

    outputNodes.reset();
    OutputNode out = outputNodes.get(0);
    out.node = 0;
    out.inputNodes.add(0);
    out.startOffset = 0;
    outputFrom = 0;
    //retOutputFrom = -1;
    lastOutputFrom = -1;
    done = false;
    finalPosInc = -1;
    finalOffset = -1;
    lastStartOffset = 0;
    maxLookaheadUsed = 0;
  }

  /** For testing */
  public int getMaxLookaheadUsed() {
    return maxLookaheadUsed;
  }
}
