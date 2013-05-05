package org.apache.lucene.analysis.kr.morph;

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
import java.util.List;

public class WSCandidateComparator implements Comparator<WSOutput> {

	public int compare(WSOutput o1, WSOutput o2) {		
		
		int end = o2.getLastEnd() - o1.getLastEnd();
		if(end!=0) return end;
		
		int s1 = o1.getPhrases().size()==0 ? 999999999 : o1.getPhrases().size();
		int s2 = o2.getPhrases().size()==0 ? 999999999 : o2.getPhrases().size();
		
		int size = s1-s2;		
		if(size!=0) return size;
				
		int score = calculateScore(o2)-calculateScore(o1);
		if(score!=0) return score;
					
		return 0;
	}

	private int calculateScore(WSOutput o) {		
				
		List<AnalysisOutput> entries = o.getPhrases();
		
		if(entries.size()==0) return 0;
		
		int sum = 0;
		for(int i=0;i<entries.size();i++) {
			sum += entries.get(i).getScore();
		}
	
		return sum / entries.size();
	}
	
}
