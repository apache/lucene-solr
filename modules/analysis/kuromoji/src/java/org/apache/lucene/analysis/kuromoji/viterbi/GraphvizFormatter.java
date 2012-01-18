package org.apache.lucene.analysis.kuromoji.viterbi;

/**
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.kuromoji.dict.ConnectionCosts;
import org.apache.lucene.analysis.kuromoji.viterbi.ViterbiNode.Type;

public class GraphvizFormatter {
  
  private final static String BOS_LABEL = "BOS";
  
  private final static String EOS_LABEL = "EOS";
  
  private final static String FONT_NAME = "Helvetica";
  
  private ConnectionCosts costs;
  
  private Map<String, ViterbiNode> nodeMap;
  
  private Map<String, String> bestPathMap;
  
  private boolean foundBOS;
  
  public GraphvizFormatter(ConnectionCosts costs) {
    this.costs = costs;
    this.nodeMap = new HashMap<String, ViterbiNode>();
    this.bestPathMap = new HashMap<String, String>();
  }
  
  public String format(ViterbiNode[][] startsArray, ViterbiNode[][] endsArray) {
    initBestPathMap(null);
    
    StringBuilder sb = new StringBuilder();
    sb.append(formatHeader());
    sb.append(formatNodes(startsArray, endsArray));
    sb.append(formatTrailer());
    return sb.toString();
  }
  
  public String format(ViterbiNode[][] startsArray, ViterbiNode[][] endsArray, List<ViterbiNode> bestPath) {
    
    //		List<ViterbiNode> bestPathWithBOSAndEOS = new ArrayList<ViterbiNode>(bastPath);
    initBestPathMap(bestPath);
    
    StringBuilder sb = new StringBuilder();
    sb.append(formatHeader());
    sb.append(formatNodes(startsArray, endsArray));
    sb.append(formatTrailer());
    return sb.toString();
    
  }
  
  private void initBestPathMap(List<ViterbiNode> bestPath) {
    this.bestPathMap.clear();
    
    if (bestPath == null){
      return;
    }
    for (int i = 0; i < bestPath.size() - 1; i++) {
      ViterbiNode from = bestPath.get(i);
      ViterbiNode to = bestPath.get(i + 1);
      
      String fromId = getNodeId(from);
      String toId = getNodeId(to);
      
      assert this.bestPathMap.containsKey(fromId) == false;
      assert this.bestPathMap.containsValue(toId) == false;
      this.bestPathMap.put(fromId, toId);
    }
  }
  
  private String formatNodes(ViterbiNode[][] startsArray, ViterbiNode[][] endsArray) {
    this.nodeMap.clear();
    this.foundBOS = false;
    
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < endsArray.length; i++) {
      if(endsArray[i] == null || startsArray[i] == null) {
        continue;
      }
      for (int j = 0; j < endsArray[i].length; j++) {
        ViterbiNode from = endsArray[i][j];
        if(from == null){
          continue;
        }
        sb.append(formatNodeIfNew(from));
        for (int k = 0; k < startsArray[i].length; k++) {
          ViterbiNode to = startsArray[i][k];
          if(to == null){
            break;
          }
          sb.append(formatNodeIfNew(to));
          sb.append(formatEdge(from, to));
        }
      }
    }
    return sb.toString();
  }
  
  private String formatNodeIfNew(ViterbiNode node) {
    String nodeId = getNodeId(node);
    if (! this.nodeMap.containsKey(nodeId)) {
      this.nodeMap.put(nodeId, node);
      return formatNode(node);
    } else {
      return "";
    }
  }	
  
  private String formatHeader() {
    StringBuilder sb = new StringBuilder();
    sb.append("digraph viterbi {\n");
    sb.append("graph [ fontsize=30 labelloc=\"t\" label=\"\" splines=true overlap=false rankdir = \"LR\" ];\n");
    sb.append("# A2 paper size\n");
    sb.append("size = \"34.4,16.5\";\n");
    sb.append("# try to fill paper\n");
    sb.append("ratio = fill;\n");
    sb.append("edge [ fontname=\"" + FONT_NAME + "\" fontcolor=\"red\" color=\"#606060\" ]\n");
    sb.append("node [ style=\"filled\" fillcolor=\"#e8e8f0\" shape=\"Mrecord\" fontname=\"" + FONT_NAME + "\" ]\n");
    
    return sb.toString();
  }
  
  private String formatTrailer() {
    return "}";
  }
  
  
  private String formatEdge(ViterbiNode from, ViterbiNode to) {
    if (this.bestPathMap.containsKey(getNodeId(from)) &&
        this.bestPathMap.get(getNodeId(from)).equals(getNodeId(to))) {
      return formatEdge(from, to, "color=\"#40e050\" fontcolor=\"#40a050\" penwidth=3 fontsize=20 ");
      
    } else {
      return formatEdge(from, to, "");
    }
  }
  
  
  private String formatEdge(ViterbiNode from, ViterbiNode to, String attributes) {
    StringBuilder sb = new StringBuilder();
    sb.append(getNodeId(from));
    sb.append(" -> ");
    sb.append(getNodeId(to));
    sb.append(" [ ");
    sb.append("label=\"");
    sb.append(getCost(from, to));
    sb.append("\"");
    sb.append(" ");
    sb.append(attributes);
    sb.append(" ");
    sb.append(" ]");
    sb.append("\n");
    return sb.toString();
  }
  
  private String formatNode(ViterbiNode node) {
    StringBuilder sb = new StringBuilder();
    sb.append("\"");
    sb.append(getNodeId(node));
    sb.append("\"");
    sb.append(" [ ");
    sb.append("label=");
    sb.append(formatNodeLabel(node));
    sb.append(" ]");
    return sb.toString();
  }
  
  private String formatNodeLabel(ViterbiNode node) {
    StringBuilder sb = new StringBuilder();
    sb.append("<<table border=\"0\" cellborder=\"0\">");
    sb.append("<tr><td>");
    sb.append(getNodeLabel(node));
    sb.append("</td></tr>");
    sb.append("<tr><td>");
    sb.append("<font color=\"blue\">");
    sb.append(node.getWordCost());
    sb.append("</font>");
    sb.append("</td></tr>");
    //		sb.append("<tr><td>");
    //		sb.append(this.dictionary.get(node.getWordId()).getPosInfo());
    //		sb.append("</td></tr>");
    sb.append("</table>>");
    return sb.toString();
  }
  
  private String getNodeId(ViterbiNode node) {
    return String.valueOf(node.hashCode());
  }
  
  private String getNodeLabel(ViterbiNode node) {
    if (node.getType() == Type.KNOWN && node.getWordId() == 0) {
      if (this.foundBOS) {
        return EOS_LABEL;
      } else {
        this.foundBOS = true;
        return BOS_LABEL;
      }
    } else {
      return node.getSurfaceFormString();
    }
  }
  
  private int getCost(ViterbiNode from, ViterbiNode to) {
    return this.costs.get(from.getLeftId(), to.getRightId());
  }
}
