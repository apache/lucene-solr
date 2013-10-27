package org.apache.lucene.analysis.ko.morph;

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

import java.util.Comparator;

class AnalysisOutputComparator<T> implements Comparator<T> {
  public int compare(T o1, T o2) {
    
    AnalysisOutput out1 = (AnalysisOutput)o1;
    AnalysisOutput out2 = (AnalysisOutput)o2;
    
    int score = out2.getScore()-out1.getScore();
    int pattern = out2.getPatn()-out1.getPatn();
    int len = out1.getStem().length()-out2.getStem().length();
    
    if (score != 0) {
      return score;
    }
    
    if (out2.getScore() == AnalysisOutput.SCORE_CORRECT &&
        out1.getScore() == AnalysisOutput.SCORE_CORRECT) {
      pattern = out1.getPatn() == PatternConstants.PTN_N || out1.getPatn() == PatternConstants.PTN_AID ? -1 : pattern;
      pattern = out2.getPatn() == PatternConstants.PTN_N || out2.getPatn() == PatternConstants.PTN_AID ? 1 : pattern;
    }
    
    if (pattern != 0) {
      return pattern;
    }
    
    if (out2.getScore() == AnalysisOutput.SCORE_COMPOUNDS &&
        out1.getScore() == AnalysisOutput.SCORE_COMPOUNDS) {
      if (out2.getMaxWordLen() != out1.getMaxWordLen()) {
        return out2.getMaxWordLen()-out1.getMaxWordLen();
      }
      if (out2.getDicWordLen() != out1.getDicWordLen()) {
        return out2.getDicWordLen()-out1.getDicWordLen();
      }
    }
    
    if (out2.getPatn() == out1.getPatn()) {
      len = out2.getStem().length()-out1.getStem().length();
    }
    
    return len;
  }
}
