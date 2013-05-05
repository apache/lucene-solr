package org.apache.lucene.analysis.kr.utils;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.kr.morph.AnalysisOutput;
import org.apache.lucene.analysis.kr.morph.MorphException;
import org.apache.lucene.analysis.kr.morph.PatternConstants;
import org.apache.lucene.analysis.kr.morph.WordEntry;

public class VerbUtil {

	public static final Map verbSuffix = new HashMap();
	
	public static final Map XVerb = new HashMap();
	
	static {
		String[] suffixs = {
				  "이","하","되","내", "나", "스럽","시키","있","없","같","당하","만하","드리","받","짓"};
		for(int i=0;i<suffixs.length;i++) verbSuffix.put(suffixs[i], suffixs[i]);
		
		String[] xverbs = {"오","내","주","보","지","오르","올리"};
		for(int i=0;i<xverbs.length;i++) XVerb.put(xverbs[i], xverbs[i]);
	}
	
	/**
	 * 어간이 용언화접미사로 끝나면 index 를 반환한다.  아니면 -1을 반환한다.
	 * @param result
	 * @return
	 */
   public static int endsWithVerbSuffix(String stem) {
	    int len = stem.length();
	    if(len<2) return -1;
	    int start = 2;
	    if(len==2) start = 1;	    
		for(int i=start;i>0;i--) { // suffix 의 가장 긴 글자수가 2이다.
			if(verbSuffix.get(stem.substring(len-i))!=null) return (len-i);
		}		
		return -1;
   }
   
   /**
    * 어간부에 보조용언 [하,되,오,내,주,지]가 있는지 조사한다.
    * @param stem
    * @return
    */
   public static int endsWithXVerb(String stem) {
	    int len = stem.length();
	    if(len<2) return -1;
	    int start = 2;
	    if(len==2) start = 1;
		for(int i=start;i>0;i--) { //xverbs 의 가장 긴 글자수는 2이다.
			if(XVerb.get(stem.substring(len-i))!=null) return (len-i);
		}
	   return -1;
   }
   
   public static boolean verbSuffix(String stem) {

	   return verbSuffix.get(stem)!=null;
	   
   }
   
   public static boolean constraintVerb(String start, String end) {
	   
	   char[] schs = MorphUtil.decompose(start.charAt(start.length()-1));
	   char[] echs = MorphUtil.decompose(end.charAt(0));
	   
	   if(schs.length==3&&schs[2]=='ㄹ'&&echs[0]=='ㄹ') return false;
	   
	   return true;
   }
   
   /**
    * 3. 학교에서이다 : 체언 + '에서/부터/에서부터' + '이' + 어미 (PTN_NJCM) <br>
    */
   public static boolean ananlysisNJCM(AnalysisOutput o, List candidates) throws MorphException {
 
	   int strlen = o.getStem().length();
	   boolean success = false;
	   
	   if(strlen>3&&(o.getStem().endsWith("에서이")||o.getStem().endsWith("부터이"))) {
		   o.addElist(o.getStem().substring(strlen-1));
		   o.setJosa(o.getStem().substring(strlen-3,strlen-1));
		   o.setStem(o.getStem().substring(0,strlen-3));
		   success = true;
	   }else if(strlen>5&&(o.getStem().endsWith("에서부터이"))) {
		   o.addElist(o.getStem().substring(strlen-1));
		   o.setJosa(o.getStem().substring(strlen-5,strlen-1));
		   o.setStem(o.getStem().substring(0,strlen-5));
		   success = true;
	   }
	   if(!success) return false;
	   
	   if(success&&DictionaryUtil.getNoun(o.getStem())!=null) {		   
		   o.setScore(AnalysisOutput.SCORE_CORRECT);
//	   }else {
//			NounUtil.confirmCNoun(o);
	   }
	   
	   o.setPatn(PatternConstants.PTN_NJCM);
	   o.setPos(PatternConstants.POS_NOUN);	
	   candidates.add(o);
	   
	   return true;
   }
   
   /**
    * 어미부와 어간부가 분리된 상태에서 용언화접미사가 결합될 수 있는지 조사한다.
    * @param o	어미부와 어간부가 분리된 결과
    * @param candidates
    * @return
    * @throws MorphException
    */
   public static boolean ananlysisNSM(AnalysisOutput o, List candidates) throws MorphException {

	    if(o.getStem().endsWith("스러우")) o.setStem(o.getStem().substring(0,o.getStem().length()-3)+"스럽");
		int idxVbSfix = VerbUtil.endsWithVerbSuffix(o.getStem());
		if(idxVbSfix<1) return false;
	
		o.setVsfx(o.getStem().substring(idxVbSfix));
		o.setStem(o.getStem().substring(0,idxVbSfix));
		o.setPatn(PatternConstants.PTN_NSM);
		o.setPos(PatternConstants.POS_NOUN);
		
		WordEntry entry = DictionaryUtil.getWordExceptVerb(o.getStem());
				
//		if(entry==null&&NounUtil.confirmCNoun(o)&&o.getCNounList().size()>0)	{
//			entry = DictionaryUtil.getNoun(o.getCNounList().get(o.getCNounList().size()-1).getWord());
//		}

//		if(entry==null) return false;
//		if(entry==null) {
//			NounUtil.confirmDNoun(o);
//			if(o.getScore()!=AnalysisOutput.SCORE_CORRECT) return false;
//		}

		if(entry!=null) {
			if(entry.getFeature(WordEntry.IDX_NOUN)=='0') return false;
			else if(o.getVsfx().equals("하")&&entry.getFeature(WordEntry.IDX_DOV)!='1') return false;
			else if(o.getVsfx().equals("되")&&entry.getFeature(WordEntry.IDX_BEV)!='1') return false;
			else if(o.getVsfx().equals("내")&&entry.getFeature(WordEntry.IDX_NE)!='1') return false;
			o.setScore(AnalysisOutput.SCORE_CORRECT); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.			
		}else {
			o.setScore(AnalysisOutput.SCORE_ANALYSIS); // '입니다'인 경우 인명 등 미등록어가 많이 발생되므로 분석성공으로 가정한다.
		}
	
		candidates.add(o);

		return true;

   }
   
   public static boolean ananlysisNSMXM(AnalysisOutput o, List candidates) throws MorphException {
   
		int idxXVerb = VerbUtil.endsWithXVerb(o.getStem());
		if(idxXVerb==-1) return false;
		
		String eogan = o.getStem().substring(0,idxXVerb);
		String[] stomis = null;

		if((eogan.endsWith("아")||eogan.endsWith("어"))&&eogan.length()>1)
			stomis = EomiUtil.splitEomi(eogan.substring(0,eogan.length()-1),eogan.substring(eogan.length()-1));
		else
			stomis = EomiUtil.splitEomi(eogan,"");

		if(stomis[0]==null) return false;
		
		o.addElist(stomis[1]);
		int idxVbSfix = VerbUtil.endsWithVerbSuffix(stomis[0]);
		if(idxVbSfix==-1) return false;
		
		o.setXverb(o.getStem().substring(idxXVerb));
		o.setVsfx(stomis[0].substring(idxVbSfix));
		o.setStem(stomis[0].substring(0,idxVbSfix));
		o.setPatn(PatternConstants.PTN_NSMXM);
		o.setPos(PatternConstants.POS_NOUN);
		WordEntry entry = DictionaryUtil.getNoun(o.getStem());
//		if(entry==null&&NounUtil.confirmCNoun(o)&&o.getCNounList().size()>0)	{
//			entry = DictionaryUtil.getNoun(o.getCNounList().get(o.getCNounList().size()-1));
//		}
		if(entry==null) return false;	
		
		if(o.getVsfx().equals("하")&&entry.getFeature(WordEntry.IDX_DOV)!='1') return false;
		if(o.getVsfx().equals("되")&&entry.getFeature(WordEntry.IDX_BEV)!='1') return false;				
		o.setScore(AnalysisOutput.SCORE_CORRECT);
		
		candidates.add(o);						

		
	   return true;	   
   }
   
   public static boolean analysisVMCM(AnalysisOutput o, List candidates) throws MorphException {
   
	   int strlen = o.getStem().length();
	   
	   if(strlen<2) return false;
	   
	   if(!o.getStem().endsWith("이")) return false;
	   
	   char[] chrs = MorphUtil.decompose(o.getStem().charAt(strlen-2));
	   boolean success = false;
	
	   if(strlen>2&&o.getStem().endsWith("기이")) {
		   o.setStem(o.getStem().substring(0,strlen-2));
		   o.addElist("기");	   
		   success = true;		   
	   } else if(chrs.length>2&&chrs[2]=='ㅁ'){
		   String[] eres = EomiUtil.splitEomi(o.getStem().substring(0,strlen-1), "");
			if(eres[0]==null) return false;
			
		   o.addElist(eres[1]);		   
		   String[] irrs = IrregularUtil.restoreIrregularVerb(eres[0], eres[1]);
		   
		   if(irrs!=null) o.setStem(irrs[0]);
		   else o.setStem(eres[0]);

		   success = true;
	   }
	   
	   if(success) {		
	   
		   o.addElist("이");
			if(DictionaryUtil.getVerb(o.getStem())!=null) {
				o.setPos(PatternConstants.POS_VERB);
				o.setPatn(PatternConstants.PTN_VMCM);
				o.setScore(AnalysisOutput.SCORE_CORRECT);
				candidates.add(o);
				return true;
			}		   
	   }
	   
	   return false;
	   
   }
   
   /**
    * 
    * 6. 도와주다 : 용언 + '아/어' + 보조용언 + 어미 (PTN_VMXM)
    * 
    * @param o
    * @param candidates
    * @return
    * @throws MorphException
    */
   public static boolean analysisVMXM(AnalysisOutput o, List candidates) throws MorphException {

		int idxXVerb = VerbUtil.endsWithXVerb(o.getStem());

		if(idxXVerb==-1) return false;
			
		o.setXverb(o.getStem().substring(idxXVerb));
		
		String eogan = o.getStem().substring(0,idxXVerb);

		String[] stomis = null;
		if(eogan.endsWith("아")||eogan.endsWith("어")) {
			stomis = EomiUtil.splitEomi(eogan.substring(0,eogan.length()-1),eogan.substring(eogan.length()-1));
			if(stomis[0]==null) return false;
		}else {
			stomis =  EomiUtil.splitEomi(eogan, "");			
			if(stomis[0]==null||!(stomis[1].startsWith("아")||stomis[1].startsWith("어"))) return false;
		}

		String[] irrs = IrregularUtil.restoreIrregularVerb(stomis[0], stomis[1]);
		if(irrs!=null) {
			o.setStem(irrs[0]);
			o.addElist(irrs[1]);
		} else {
			o.setStem(stomis[0]);
			o.addElist(stomis[1]);
		}

		if(DictionaryUtil.getVerb(o.getStem())!=null) {
			o.setPos(PatternConstants.POS_VERB);
			o.setPatn(PatternConstants.PTN_VMXM);
			o.setScore(AnalysisOutput.SCORE_CORRECT);
			candidates.add(o);
			return true;
		}	

		return false;	   
   }
    
}
