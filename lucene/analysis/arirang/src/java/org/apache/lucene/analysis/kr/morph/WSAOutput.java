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

import java.util.ArrayList;
import java.util.List;

public class WSAOutput {

	private String source;
	
	private List<AnalysisOutput> results;
	
	private int wds = 0;
	
	private int end = 0;

	public WSAOutput() {
		results = new ArrayList();
	}
	
	public WSAOutput(String src) {
		source = src;
		results = new ArrayList();
	}
	
	public WSAOutput(String src, List list) {
		source = src;		
		results = list;
	}
	
	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public List getResults() {
		return results;
	}

	public void setResults(List results) {
		this.results = results;
	}
		
	public void addNounResults(String word) {		
		addNounResults(word, null);		
	}
	
	public void addNounResults(String word, String end) {		
		addNounResults(word, end, AnalysisOutput.SCORE_ANALYSIS);		
	}
	
	public void addNounResults(String word, String end, int score) {	
		
		AnalysisOutput output = new AnalysisOutput(word, end, null, PatternConstants.PTN_NJ);
		if(end==null) output.setPatn(PatternConstants.PTN_N);
		
		output.setPos(PatternConstants.POS_NOUN);
		output.setScore(score);
		
		this.results.add(output);	
	}	
	
}
