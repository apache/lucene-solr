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
package org.apache.lucene.analysis.ko;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.ko.KoreanTokenizer.Position;
import org.apache.lucene.analysis.ko.KoreanTokenizer.WrappedPositionArray;
import org.apache.lucene.analysis.ko.dict.ConnectionCosts;
import org.apache.lucene.analysis.ko.dict.Dictionary;


// TODO: would be nice to show 2nd best path in a diff't
// color...

/**
 * Outputs the dot (graphviz) string for the viterbi lattice.
 */
public class GraphvizFormatter {
  
  private final static String BOS_LABEL = "BOS";
  
  private final static String EOS_LABEL = "EOS";
  
  private final static String FONT_NAME = "Helvetica";
  
  private final ConnectionCosts costs;
  
  private final Map<String, String> bestPathMap;
  
  private final StringBuilder sb = new StringBuilder();
  
  public GraphvizFormatter(ConnectionCosts costs) {
    this.costs = costs;
    this.bestPathMap = new HashMap<>();
    sb.append(formatHeader());
    sb.append("  init [style=invis]\n");
    sb.append("  init -> 0.0 [label=\"" + BOS_LABEL + "\"]\n");
  }

  public String finish() {
    sb.append(formatTrailer());
    return sb.toString();
  }

  // Backtraces another incremental fragment:
  void onBacktrace(KoreanTokenizer tok, WrappedPositionArray positions, int lastBackTracePos, Position endPosData, int fromIDX, char[] fragment, boolean isEnd) {
    setBestPathMap(positions, lastBackTracePos, endPosData, fromIDX);
    sb.append(formatNodes(tok, positions, lastBackTracePos, endPosData, fragment));
    if (isEnd) {
      sb.append("  fini [style=invis]\n");
      sb.append("  ");
      sb.append(getNodeID(endPosData.pos, fromIDX));
      sb.append(" -> fini [label=\"" + EOS_LABEL + "\"]");
    }
  }

  // Records which arcs make up the best bath:
  private void setBestPathMap(WrappedPositionArray positions, int startPos, Position endPosData, int fromIDX) {
    bestPathMap.clear();

    int pos = endPosData.pos;
    int bestIDX = fromIDX;
    while (pos > startPos) {
      final Position posData = positions.get(pos);

      final int backPos = posData.backPos[bestIDX];
      final int backIDX = posData.backIndex[bestIDX];

      final String toNodeID = getNodeID(pos, bestIDX);
      final String fromNodeID = getNodeID(backPos, backIDX);
      
      assert !bestPathMap.containsKey(fromNodeID);
      assert !bestPathMap.containsValue(toNodeID);
      bestPathMap.put(fromNodeID, toNodeID);
      pos = backPos;
      bestIDX = backIDX;
    }
  }
  
  private String formatNodes(KoreanTokenizer tok, WrappedPositionArray positions, int startPos, Position endPosData, char[] fragment) {

    StringBuilder sb = new StringBuilder();
    // Output nodes
    for (int pos = startPos+1; pos <= endPosData.pos; pos++) {
      final Position posData = positions.get(pos);
      for(int idx=0;idx<posData.count;idx++) {
        sb.append("  ");
        sb.append(getNodeID(pos, idx));
        sb.append(" [label=\"");
        sb.append(pos);
        sb.append(": ");
        sb.append(posData.lastRightID[idx]);
        sb.append("\"]\n");
      }
    }

    // Output arcs
    for (int pos = endPosData.pos; pos > startPos; pos--) {
      final Position posData = positions.get(pos);
      for(int idx=0;idx<posData.count;idx++) {
        final Position backPosData = positions.get(posData.backPos[idx]);
        final String toNodeID = getNodeID(pos, idx);
        final String fromNodeID = getNodeID(posData.backPos[idx], posData.backIndex[idx]);

        sb.append("  ");
        sb.append(fromNodeID);
        sb.append(" -> ");
        sb.append(toNodeID);

        final String attrs;
        if (toNodeID.equals(bestPathMap.get(fromNodeID))) {
          // This arc is on best path
          attrs = " color=\"#40e050\" fontcolor=\"#40a050\" penwidth=3 fontsize=20";
        } else {
          attrs = "";
        }

        final Dictionary dict = tok.getDict(posData.backType[idx]);
        final int wordCost = dict.getWordCost(posData.backID[idx]);
        final int bgCost = costs.get(backPosData.lastRightID[posData.backIndex[idx]],
                                     dict.getLeftId(posData.backID[idx]));

        final String surfaceForm = new String(fragment,
                                              posData.backPos[idx] - startPos,
                                              pos - posData.backPos[idx]);
        
        sb.append(" [label=\"");
        sb.append(surfaceForm);
        sb.append(' ');
        sb.append(wordCost);
        if (bgCost >= 0) {
          sb.append('+');
        }
        sb.append(bgCost);
        sb.append("\"");
        sb.append(attrs);
        sb.append("]\n");
      }
    }
    return sb.toString();
  }
  
  private String formatHeader() {
    return "digraph viterbi {\n" +
        "  graph [ fontsize=30 labelloc=\"t\" label=\"\" splines=true overlap=false rankdir = \"LR\"];\n" +
        //sb.append("  // A2 paper size\n");
        //sb.append("  size = \"34.4,16.5\";\n");
        //sb.append("  // try to fill paper\n");
        //sb.append("  ratio = fill;\n");
        "  edge [ fontname=\"" + FONT_NAME + "\" fontcolor=\"red\" color=\"#606060\" ]\n" +
        "  node [ style=\"filled\" fillcolor=\"#e8e8f0\" shape=\"Mrecord\" fontname=\"" + FONT_NAME + "\" ]\n";
  }
  
  private String formatTrailer() {
    return "}";
  }
  
  private String getNodeID(int pos, int idx) {
    return pos + "." + idx;
  }
}
