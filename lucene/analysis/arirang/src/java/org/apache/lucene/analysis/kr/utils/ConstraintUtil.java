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
import java.util.Map;

import org.apache.lucene.analysis.kr.morph.PatternConstants;

/**
 * 결합이 가능한 조건을 처리하는 클래스
 */
public class ConstraintUtil {

	private static Map hahes = new HashMap(); // "글로벌화해 ", "민족화해" 처럼 화해와 결합이 가능한 명사
	static {
		hahes.put("민족", "Y");hahes.put("동서", "Y");hahes.put("남북", "Y");
	}
	
	private static Map eomiPnouns = new HashMap(); 
	static {
		eomiPnouns.put("ㄴ", "Y");eomiPnouns.put("ㄹ", "Y");eomiPnouns.put("ㅁ", "Y");
	}
	
	private static Map PTN_MLIST= new HashMap();
	static {
		PTN_MLIST.put(PatternConstants.PTN_NSM, PatternConstants.PTN_NSM);
		PTN_MLIST.put(PatternConstants.PTN_NSMXM, PatternConstants.PTN_NSMXM);
		PTN_MLIST.put(PatternConstants.PTN_NJCM, PatternConstants.PTN_NJCM);
		PTN_MLIST.put(PatternConstants.PTN_VM, PatternConstants.PTN_VM);
		PTN_MLIST.put(PatternConstants.PTN_VMCM, PatternConstants.PTN_VMCM);
		PTN_MLIST.put(PatternConstants.PTN_VMXM, PatternConstants.PTN_VMXM);
		PTN_MLIST.put(PatternConstants.PTN_NVM, PatternConstants.PTN_NVM);
	}
	
	private static Map PTN_JLIST= new HashMap();
	static {
		PTN_JLIST.put(PatternConstants.PTN_NJ, PatternConstants.PTN_NJ);
		PTN_JLIST.put(PatternConstants.PTN_NSMJ, PatternConstants.PTN_NSMJ);
		PTN_JLIST.put(PatternConstants.PTN_VMJ, PatternConstants.PTN_VMJ);
		PTN_JLIST.put(PatternConstants.PTN_VMXMJ, PatternConstants.PTN_VMXMJ);
	}
	
	private static Map WORD_GUKS= new HashMap();
	static {
		WORD_GUKS.put("날것", "Y");
		WORD_GUKS.put("들것", "Y");
		WORD_GUKS.put("별것", "Y");
		WORD_GUKS.put("찰것", "Y");
		WORD_GUKS.put("탈것", "Y");
		WORD_GUKS.put("하잘것", "Y");
	}
	
	// 종성이 있는 음절과 연결될 수 없는 조사
	private static Map JOSA_TWO= new HashMap();
	static {
		JOSA_TWO.put("가", "Y");
		JOSA_TWO.put("는", "Y");
		JOSA_TWO.put("다", "Y");
		JOSA_TWO.put("나", "Y");
		JOSA_TWO.put("니", "Y");
		JOSA_TWO.put("고", "Y");
		JOSA_TWO.put("라", "Y");
		JOSA_TWO.put("와", "Y");
		JOSA_TWO.put("랑", "Y");
		JOSA_TWO.put("를", "Y");
		JOSA_TWO.put("며", "Y");
		JOSA_TWO.put("든", "Y");
		JOSA_TWO.put("야", "Y");
		JOSA_TWO.put("여", "Y");
	}
	
	// 종성이 없는 음절과 연결될 수 없는 조사
	private static Map JOSA_THREE= new HashMap();
	static {
		JOSA_THREE.put("과", "Y");
		JOSA_THREE.put("은", "Y");
		JOSA_THREE.put("아", "Y");
		JOSA_THREE.put("으", "Y");
		JOSA_THREE.put("은", "Y");
		JOSA_THREE.put("을", "Y");
	}
	
	public static boolean canHaheCompound(String key) {
		if(hahes.get(key)!=null) return true;
		return false;
	}
		
	/**
	 * 어미가 ㄴ,ㄹ,ㅁ 으로 끝나는지 조사한다.
	 * @param eomi
	 * @return
	 */
	public static boolean isNLM(String eomi) {
		
		if(eomi==null || "".equals(eomi)) return false;
		
		if(eomiPnouns.get(eomi)!=null) return true;
		
		char[] chrs = MorphUtil.decompose(eomi.charAt(eomi.length()-1));
		if(chrs.length==3  && eomiPnouns.get(Character.toString(chrs[2]))!=null) return true;
		
		return true;
		
	}
	
	public static boolean isEomiPhrase(int ptn) {
		
		if(PTN_MLIST.get(ptn)!=null) return true;
		
		return false;
		
	}
	
	public static boolean isJosaNounPhrase(int ptn) {
		
		if(PTN_JLIST.get(ptn)!=null) return true;
		
		return false;
		
	}
	
	public static boolean isJosaAdvPhrase(int ptn) {
		
		if(PatternConstants.PTN_ADVJ==ptn) return true;
		
		return false;
		
	}
	
	public static boolean isAdvPhrase(int ptn) {
		
		if(PatternConstants.PTN_ADVJ==ptn || PatternConstants.PTN_AID==ptn) return true;
		
		return false;
		
	}
	
	public static boolean isTwoJosa(String josa) {
		
		return (JOSA_TWO.get(josa)!=null);
		
	}
	public static boolean isThreeJosa(String josa) {
		
		return (JOSA_THREE.get(josa)!=null);
		
	}	
}
