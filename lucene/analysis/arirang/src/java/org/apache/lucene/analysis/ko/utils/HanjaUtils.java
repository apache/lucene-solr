package org.apache.lucene.analysis.ko.utils;

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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.ko.dic.DictionaryResources;
import org.apache.lucene.analysis.ko.morph.MorphException;

public class HanjaUtils {
  private HanjaUtils() {}

  private static final Map<Character, char[]> mapHanja;
  static {
    try {
      List<String> strList = DictionaryResources.readLines(DictionaryResources.FILE_MAP_HANJA_DIC);
      Map<Character, char[]> map = new HashMap<Character, char[]>();    
    
      for(String s : strList) {
        if(s.isEmpty() || s.indexOf(",")==-1) continue;

        String[] hanInfos = s.split("[,]+");

        if(hanInfos.length!=2) continue;
        
        map.put(hanInfos[0].charAt(0), hanInfos[1].toCharArray());
      }
      
      mapHanja = Collections.unmodifiableMap(map);
    } catch (IOException e) {
      throw new RuntimeException("Cannot load: " + DictionaryResources.FILE_MAP_HANJA_DIC);
    }
  }
  
  /**
   * 한자에 대응하는 한글을 찾아서 반환한다.
   * 하나의 한자는 여러 음으로 읽일 수 있으므로 가능한 모든 음을 한글로 반환한다.
   */
  public static char[] convertToHangul(char hanja) throws MorphException {
//    if(hanja>0x9FFF||hanja<0x3400) return new char[]{hanja};
    
    final char[] result = mapHanja.get(hanja);
    return (result==null) ? new char[]{hanja} : result;
  }
}
