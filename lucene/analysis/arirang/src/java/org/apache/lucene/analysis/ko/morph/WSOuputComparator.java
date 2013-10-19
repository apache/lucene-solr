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

class WSOuputComparator implements Comparator<AnalysisOutput> {

  public int compare(AnalysisOutput o1, AnalysisOutput o2) {
    
    // 길이의 역순으로 정렬한다.
    
    int score = o2.getScore() - o1.getScore();
    if(score!=0) return score;
    
    int len = o2.getSource().length() - o1.getSource().length();    
    if(len!=0) return len;
    
  
    int ptn = getPtnScore(o2.getPatn()) - getPtnScore(o1.getPatn());
    if(ptn!=0) return ptn;
    
    int stem = o1.getStem().length() - o2.getStem().length();
    if(stem!=0) return stem;
    
  
    return 0;
  }

  private int getPtnScore(int ptn) {
    
    if(ptn==PatternConstants.PTN_N) ptn = 7;
    else if(ptn==PatternConstants.PTN_AID) return 50;
    
    return ptn;
  }
}
