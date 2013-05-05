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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.kr.utils.DictionaryUtil;

/**
 * 복합명사를 분해한다.
 */
public class CompoundNounAnalyzer {
	
	private static int score = 1;	
	
	private boolean exactMach  = true;
	
	private static Pattern NUM_PATTERN;
	static {
		NUM_PATTERN = Pattern.compile("^[0-9\\.,]+$");
	}

	private static Pattern ALPHANUM_PATTERN;
	static {
		ALPHANUM_PATTERN = Pattern.compile("^[0-9A-Za-z\\.,]+$");
	}
		
	public boolean isExactMach() {
		return exactMach;
	}

	public void setExactMach(boolean exactMach) {
		this.exactMach = exactMach;
	}

	public List analyze(String input) throws MorphException {
		
		return analyze(input,true);
		
	}
	
	public List<CompoundEntry> analyze(String input, boolean isFirst) throws MorphException {
		
		int len = input.length();
		if(len<3) return new ArrayList<CompoundEntry>();	
		
		List<CompoundEntry> outputs = new ArrayList<CompoundEntry>();
		
		switch(len) {
			case  3 :
				analyze3Word(input,outputs,isFirst);
				break;
			case  4 :
				analyze4Word(input,outputs,isFirst);
				break;	
			case  5 :
				analyze5Word(input,outputs,isFirst);
				break;
			case  6 :
				analyze6Word(input,outputs,isFirst);
				break;	
			default :
				analyzeLongText(input,outputs,isFirst);				
		}

		return outputs;
		
	}
		
	private void analyze3Word(String input,List<CompoundEntry> outputs, boolean isFirst) throws MorphException {

		int[] units1 = {2,1};
		CompoundEntry[] entries1 = analysisBySplited(units1,input,isFirst);
		if(entries1!=null && entries1[0].isExist()&&entries1[1].isExist()) {
			outputs.addAll(Arrays.asList(entries1));
			return;		
		}

		int[] units2 = {1,2};
		CompoundEntry[] entries2 = analysisBySplited(units2,input,isFirst);
		if(entries2!=null && entries2[0].isExist()&&entries2[1].isExist()) {
			outputs.addAll(Arrays.asList(entries2));
		}
					
	}	
	
	private void analyze4Word(String input,List<CompoundEntry> outputs, boolean isFirst) throws MorphException {
	
		if(!isFirst) {
			int[] units0 = {1,3};
			CompoundEntry[] entries0 = analysisBySplited(units0,input,isFirst);
			if(entries0!=null && entries0[0].isExist()&&entries0[1].isExist()) {
				outputs.addAll(Arrays.asList(entries0));
				return;		
			}
		}
				
		int[] units2 = {1,2,1};
		CompoundEntry[] entries2 = analysisBySplited(units2,input,isFirst);	
		if(entries2!=null && entries2[0].isExist()&&entries2[1].isExist()&&entries2[2].isExist()) {
			outputs.addAll(Arrays.asList(entries2));	
			return;
		}
		
		int[] units1 = {2,2};
		CompoundEntry[] entries1 = analysisBySplited(units1,input,isFirst);
		if(entries1!=null && entries1[0].isExist()&&entries1[1].isExist()) {
			outputs.addAll(Arrays.asList(entries1));		
			return;		
		}

		if(!exactMach&&entries1!=null && (entries1[0].isExist()||entries1[1].isExist())) {
			outputs.addAll(Arrays.asList(entries1));	
		}
	}
	
	private void analyze5Word(String input,List<CompoundEntry> outputs, boolean isFirst) throws MorphException {
			
		int[] units1 = {2,3};
		CompoundEntry[] entries1 = analysisBySplited(units1,input,isFirst);
		if(entries1!=null && entries1[0].isExist()&&entries1[1].isExist()) {
			outputs.addAll(Arrays.asList(entries1));
			return;		
		}
		
		int[] units2 = {3,2};
		CompoundEntry[] entries2 = analysisBySplited(units2,input,isFirst);
		if(entries2!=null && entries2[0].isExist()&&entries2[1].isExist()) {
			outputs.addAll(Arrays.asList(entries2));
			return;		
		}
		
		int[] units_1 = {4,1};
		CompoundEntry[] entries_1 = analysisBySplited(units_1,input,isFirst);
		if(entries_1!=null && entries_1[0].isExist()&&entries_1[1].isExist()) {			
			outputs.addAll(Arrays.asList(entries_1));
			return;		
		}
		
		int[] units3 = {2,2,1};
		CompoundEntry[] entries3 = analysisBySplited(units3,input,isFirst);
		if(entries3!=null && entries3[0].isExist()&&entries3[1].isExist()&&entries3[2].isExist()) {			
			outputs.addAll(Arrays.asList(entries3));
			return;
		}
		
		int[] units4 = {2,1,2};
		CompoundEntry[] entries4 = analysisBySplited(units4,input,isFirst);
		if(entries4!=null && entries4[0].isExist()&&entries4[1].isExist()&&entries4[2].isExist()) {			
			outputs.addAll(Arrays.asList(entries4));
			return;
		}
		
		if(!exactMach&&entries1!=null && (entries1[0].isExist()||entries1[1].isExist())) {
			outputs.addAll(Arrays.asList(entries1));	
			return;
		}
		
		if(!exactMach&&entries2!=null && (entries2[0].isExist()||entries2[1].isExist())) {	
			outputs.addAll(Arrays.asList(entries2));	
			return;
		}
		
		if(!exactMach&&entries3!=null && (entries3[0].isExist()||entries3[1].isExist())) {
			outputs.addAll(Arrays.asList(entries3));	
		}	
		
		if(!exactMach&&entries4!=null && (entries4[0].isExist()||entries4[2].isExist())) {
			outputs.addAll(Arrays.asList(entries4));	
		}			
	}
	
	private void analyze6Word(String input,List<CompoundEntry> outputs, boolean isFirst) throws MorphException {
		
		int[] units3 = {2,4};
		CompoundEntry[] entries3 = analysisBySplited(units3,input,isFirst);
		if(entries3!=null && entries3[0].isExist()&&entries3[1].isExist()) {
			outputs.addAll(Arrays.asList(entries3));
			return;		
		}
		
		int[] units4 = {4,2};
		CompoundEntry[] entries4 = analysisBySplited(units4,input,isFirst);
		if(entries4!=null && entries4[0].isExist()&&entries4[1].isExist()) {
			outputs.addAll(Arrays.asList(entries4));
			return;		
		}
		
		int[] units2 = {3,3};
		CompoundEntry[] entries2 = analysisBySplited(units2,input,isFirst);
		if(entries2!=null && entries2[0].isExist()&&entries2[1].isExist()) {
			outputs.addAll(Arrays.asList(entries2));
			return;		
		}
		
		int[] units6 = {3,2,1};
		CompoundEntry[] entries6 = analysisBySplited(units6,input,isFirst);
		if(entries6!=null && entries6[0].isExist()&&entries6[1].isExist()) {
			outputs.addAll(Arrays.asList(entries6));
			return;		
		}
		
		int[] units7 = {2,3,1};
		CompoundEntry[] entries7 = analysisBySplited(units7,input,isFirst);
		if(entries7!=null && entries7[0].isExist()&&entries7[1].isExist()) {
			outputs.addAll(Arrays.asList(entries7));
			return;		
		}
		
		int[] units1 = {2,2,2};
		CompoundEntry[] entries1 = analysisBySplited(units1,input,isFirst);
		if(entries1!=null && entries1[0].isExist()&&entries1[1].isExist()&&entries1[2].isExist()) {
			outputs.addAll(Arrays.asList(entries1));
			return;		
		}				
		
		int[] units5 = {2,1,2,1};
		CompoundEntry[] entries5 = analysisBySplited(units5,input,isFirst);
		if(entries5!=null && entries5[0].isExist()&&entries5[1].isExist()&&entries5[2].isExist()&&entries5[3].isExist()) {
			outputs.addAll(Arrays.asList(entries5));
			return;		
		}
		
		if(!exactMach&&entries1!=null && canCompound(entries1,2*score)) {
			outputs.addAll(Arrays.asList(entries1));
			return;		
		}
		
		if(!exactMach&&entries2!=null && (entries2[0].isExist()||entries2[1].isExist())) {
			outputs.addAll(Arrays.asList(entries2));
			return;		
		}
		
		if(!exactMach&&entries3!=null && (entries3[0].isExist()||entries3[1].isExist())) {
			outputs.addAll(Arrays.asList(entries3));
			return;		
		}	
		
		if(!exactMach&&entries4!=null && (entries4[0].isExist()||entries4[1].isExist())) {
			outputs.addAll(Arrays.asList(entries4));	
		}	
			
	}
	
	private void analyzeLongText(String input,List<CompoundEntry> outputs, boolean isFirst) throws MorphException {
	
		int pos = input.length()/2;
		if(input.length()%2==1) pos++;
		
		if(input.length()>20) return; // 20글자 이상의 복합명사는 무시함.
		
		int score = 0;
		List results = new ArrayList();
		boolean hasContain = false;
		
		for(int i=pos;i>=2;i--) {
			
			String prev = input.substring(0,i);
			String rear = input.substring(i);
			
			List<CompoundEntry> candidates = new ArrayList();
			
			CompoundEntry prevEntry = analyzeSingle(prev);
			if(prevEntry.isExist()) {
				candidates.add(prevEntry);
			} else {
				List list = analyze(prev, true);
				if(list.size()==0) {
					candidates.add(prevEntry);
				} else {
					candidates.addAll(list);
				}
			}
			
			CompoundEntry e = candidates.get(candidates.size()-1);		
			if(!hasContain&&containWord(e.getWord(),input,i)) {
				i -= e.getWord().length()-1;
				hasContain=true;
				continue;
			}

			CompoundEntry rearEntry = analyzeSingle(rear);
			if(rearEntry.isExist()||rear.length()==3) {
				candidates.add(rearEntry);
			} else {
				List<CompoundEntry> list = analyze(rear, false);
				
				if(list.size()==0) {
					if(!e.isExist())
						candidates.set(candidates.size()-1, analyzeSingle(e.getWord()+rearEntry.getWord()));
					else
						candidates.add(rearEntry);
				} else {
					if(!e.isExist()) 
						candidates.set(candidates.size()-1, analyzeSingle(e.getWord()+list.remove(0).getWord()));
					candidates.addAll(list);
				}
			}
			
			int eval = evaluation(candidates);

			if(results==null || score<eval || 
					(score==eval && results.size()>candidates.size()) )  {
				results = candidates;
				score = eval;
			}
	
			if(eval==110) break;
		}

		outputs.addAll(results);
		
	}
	
	private int evaluation(List<CompoundEntry> candidates) {
		
		int eval = 10;
		
		int one = 0;
		int exist = 0;		
		
		for(CompoundEntry entry : candidates) {
			if(entry.getWord().length()==1) one++;
			if(entry.isExist()) exist++;
		}
		
		if(one>3) return eval;
		
		eval = eval + (exist*100)/candidates.size() - (one*100)/candidates.size();
		
		return eval;
	}
	
	private boolean containWord(String before, String input, int pos) throws MorphException {
		
		String prev = null;
		for(int i=pos;i<input.length();i++) {
			
			String text = before+input.substring(pos,i+1);		
			if(DictionaryUtil.findWithPrefix(text).hasNext()) {
				prev = text;
				continue;
			}
			
			if(prev!=null&&DictionaryUtil.getNoun(prev)!=null) return true;
			
			break;
		}

		return false;
		
	}
	
//	private void analyzeLongText(String input,List outputs, boolean isFirst) throws MorphException {
//		
//		if(exactMach) return;
//
//		String[] words = splitByLongestWord(input);
//		
//		if(words==null) return;
//
//		if(isFirst&&words[0]!=null&&words[0].length()==1&&!DictionaryUtil.existPrefix(words[0])) return;
//		if(words[2]!=null&&words[2].length()==1&&!DictionaryUtil.existSuffix(words[2])) return;
//	
//		CompoundEntry e1 = null;
//		List list1 = null;
//		boolean success1 = false;
//		if(words[0]!=null) {
//			e1 = analyzeSingle(words[0]);
//			if(!e1.isExist()) list1 = analyze(words[0],false);
//		}
//		
//		CompoundEntry e2 = null;
//		List list2 = null;
//		boolean success2 = false;
//		if(words[2]!=null) {	
//			e2 = analyzeSingle(words[2]);
//			if(!e2.isExist()) list2 = analyze(words[2],false);
//		}
//		
//		if(list1!=null&&list1.size()>0) {
//			outputs.addAll(list1);
//		}else if(e1!=null){
//			outputs.add(e1);
//		}
//		
//		outputs.add(analyzeSingle(words[1]));
//		
//		if(list2!=null&&list2.size()>0) {
//			outputs.addAll(list2);				
//		}else if(e2!=null){
//			outputs.add(e2);
//		}
//	}
	
//	private String[] splitByLongestWord(String input) throws MorphException  {
//		
//		int pos = 0;
//		String lngWord = "";
//		int last = input.length()-1;
//		boolean ftry = true;	
//		
//		for(int i=0;i<last;i++) {
//			String ss = input.substring(i);
//			String word =lookupWord(ss,ftry);			
//			if(word!=null&&validCompound(word, ss.substring(word.length()), ftry,i+1)&&lngWord.length()<word.length()) {			
//				lngWord = word;			
//				pos = i;
//				if(i==0) {
//					ftry = false;
//				}else {
//					Matcher m = ALPHANUM_PATTERN.matcher(input.substring(0,i));
//					if(!m.find()) {
//						ftry = false;
//					}					
//				}
//			}			
//		}
//		
//		if("".equals(lngWord)) return null;
//		
//		String[] results = new String[3];
//		int end = pos+lngWord.length();		
//		
//		if(pos!=0) results[0] = input.substring(0,pos);
//		results[1] = lngWord;
//
//		if(end<input.length()) results[2] = input.substring(end);
//		
//		return results;
//	}
	
//	private String lookupWord(String text, boolean ftry) throws MorphException  {
//		
//		String word = null;
//		String prev = null;
//				
//		for(int i=2;i<=text.length();i++) {
//			
//			word = text.substring(0,i);
//			Iterator prefix = DictionaryUtil.findWithPrefix(word);
//			if(prefix.hasNext()) {
//				prev = word;
//				continue;
//			}
//			
//			if(prev==null) return null;
//			
//			WordEntry entry = DictionaryUtil.getWordExceptVerb(prev);
//			if(entry!=null) {
//				String str = entry.getWord();	
//				String suffix = text.substring(i-1,i);				
//				if(ftry&&str.length()==2&&DictionaryUtil.existSuffix(suffix)) return null; // 2글자+접미어 결합이 많으므로	
//				return str;
//			}
//			
//			return null;
//		}
//		
//		WordEntry entry = DictionaryUtil.getWordExceptVerb(text);
//		if(entry!=null) return entry.getWord();
//		
//		return null;
//		
//	}
	
	private CompoundEntry[] analysisBySplited(int[] units, String input, boolean isFirst) throws MorphException {
	
		CompoundEntry[] entries = new CompoundEntry[units.length];
		
		int pos = 0;
		String prev = null;
		
		for(int i=0;i<units.length;i++) {
			
			String str = input.substring(pos,pos+units[i]);

			if(i!=0&&!validCompound(prev,str,isFirst&&(i==1),i)) return null;
			
			entries[i] = analyzeSingle(str); // CompoundEntry 로 변환

			pos += units[i];
			prev = str;
		}
		
		return entries;
		
	}
	
	private boolean canCompound(CompoundEntry[] entries, int thredhold) {
		
		int achived = 0;
		for(int i=0;i<entries.length;i++) {
			if(entries[i].isExist()) achived += score;
		}
	
		if(achived<thredhold) return false;
		
		return true;
	}
	
	/**
	 * 입력된 String 을 CompoundEntry 로 변환
	 * @param input
	 * @return
	 * @throws MorphException
	 */
	private CompoundEntry analyzeSingle(String input) throws MorphException {
						
		boolean success = false;
		int score = AnalysisOutput.SCORE_ANALYSIS;
		int ptn = PatternConstants.PTN_N;
		char pos = PatternConstants.POS_NOUN;
		if(input.length()==1) return  new CompoundEntry(input, 0, true,pos);
		
		WordEntry entry = DictionaryUtil.getWordExceptVerb(input);
		if(entry!=null) {
			score = AnalysisOutput.SCORE_CORRECT;
			if(entry.getFeature(WordEntry.IDX_NOUN)!='1') {
				ptn = PatternConstants.PTN_AID;
				pos = PatternConstants.POS_AID;
			}
		}

		return  new CompoundEntry(input, 0, score==AnalysisOutput.SCORE_CORRECT,pos);
		
	}
	
	private boolean validCompound(String before, String after, boolean isFirst, int pos) throws MorphException {

		if(pos==1&&before.length()==1&&(!isFirst||!DictionaryUtil.existPrefix(before))) return false;		

		if(after.length()==1&&!DictionaryUtil.existSuffix(after)) return false;

		if(pos!=1&&before.length()==1) {
			
			WordEntry entry1 = DictionaryUtil.getUncompound(before+after);	
			if(entry1!=null){
				List<CompoundEntry> compounds = entry1.getCompounds();
				if(before.equals(compounds.get(0).getWord())&&
						after.equals(compounds.get(1).getWord())) return false;
			}
			
		}

		WordEntry entry2 = after.length()==1 ? null : DictionaryUtil.getUncompound(after);
		if(entry2!=null){
			List<CompoundEntry> compounds = entry2.getCompounds();			
			if("*".equals(compounds.get(0).getWord())&&
					after.equals(compounds.get(1).getWord())) return false;
		}
		
		return true;
		
	}
	
}
