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

import java.util.List;


public class MorphAnalyzerManager {

	public void analyze(String strs) {
		MorphAnalyzer analyzer = new MorphAnalyzer();
		String[] tokens = strs.split(" ");
		for(String token:tokens) {
			try {
				List<AnalysisOutput> results = analyzer.analyze(token);
				for(AnalysisOutput o:results) {
					System.out.print(o.toString()+"->");
					for(int i=0;i<o.getCNounList().size();i++){
						System.out.print(o.getCNounList().get(i)+"/");
					}
					System.out.println("<"+o.getScore()+">");
				}
			} catch (MorphException e) {
				e.printStackTrace();
			}
		}
	}
}
