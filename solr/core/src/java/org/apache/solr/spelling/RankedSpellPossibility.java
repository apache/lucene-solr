package org.apache.solr.spelling;
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

import java.util.List;

public class RankedSpellPossibility implements Comparable<RankedSpellPossibility> {
	private List<SpellCheckCorrection> corrections;
	private int rank;

	//Rank poorer suggestions ahead of better ones for use with a PriorityQueue
	public int compareTo(RankedSpellPossibility rcl) {
		return new Integer(rcl.rank).compareTo(rank);		
	}

	public List<SpellCheckCorrection> getCorrections() {
		return corrections;
	}

	public void setCorrections(List<SpellCheckCorrection> corrections) {
		this.corrections = corrections;
	}

	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}
	
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("rank=").append(rank);
		if(corrections != null) {
			for(SpellCheckCorrection corr : corrections) {
				sb.append("     ");
				sb.append(corr.getOriginal()).append(">").append(corr.getCorrection()).append(" (").append(corr.getNumberOfOccurences()).append(")");
			}
		}
		return sb.toString();
	}
}
