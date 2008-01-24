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

package org.apache.solr.util;

import junit.framework.TestCase;

import java.util.*;

import org.apache.lucene.analysis.StopAnalyzer;

public class TestCharArrayMap extends TestCase {
  Random r = new Random(0);

  public void doRandom(int iter, boolean ignoreCase) {
    CharArrayMap map = new CharArrayMap(1,ignoreCase);
    HashMap hmap = new HashMap();

    char[] key;
    for (int i=0; i<iter; i++) {
      int len = r.nextInt(5);
      key = new char[len];
      for (int j=0; j<key.length; j++) {
        key[j] = (char)r.nextInt(127);
      }
      String keyStr = new String(key);
      String hmapKey = ignoreCase ? keyStr.toLowerCase() : keyStr; 

      int val = r.nextInt();

      Object o1 = map.put(key, val);
      Object o2 = hmap.put(hmapKey,val);
      assertEquals(o1,o2);

      // add it again with the string method
      assertEquals(val, map.put(keyStr,val));

      assertEquals(val, map.get(key,0,key.length));
      assertEquals(val, map.get(key));
      assertEquals(val, map.get(keyStr));

      assertEquals(hmap.size(), map.size());
    }

    assertEquals(map,hmap);
    assertEquals(hmap, map);    
  }

  public void testCharArrayMap() {
    for (int i=0; i<5; i++) {  // pump this up for more random testing
      doRandom(1000,false);
      doRandom(1000,true);      
    }
  }

  public void testMethods() {
    CharArrayMap<Integer> cm = new CharArrayMap<Integer>(2,false);
    HashMap<String,Integer> hm = new HashMap<String,Integer>();
    hm.put("foo",1);
    hm.put("bar",2);
    cm.putAll(hm);
    assertEquals(hm, cm);
    assertEquals(cm, hm);
    hm.put("baz", 3);
    assertFalse(hm.equals(cm));
    assertFalse(cm.equals(hm));
    assertTrue(cm.equals(cm));
    cm.putAll(hm);
    assertEquals(hm, cm);

    Iterator<Map.Entry<String,Integer>> iter1 = cm.entrySet().iterator();
    int n=0;
    while (iter1.hasNext()) {
      Map.Entry<String,Integer> entry = iter1.next();
      String key = entry.getKey();
      Integer val = entry.getValue();
      assertEquals(hm.get(key), val);
      entry.setValue(val*100);
      assertEquals(val*100, (int)cm.get(key));
      n++;
    }
    assertEquals(hm.size(), n);
    cm.clear();
    cm.putAll(hm);

    CharArrayMap<Integer>.EntryIterator iter2 = cm.iterator();
    n=0;
    while (iter2.hasNext()) {
      char[] keyc = iter2.nextKey();
      Integer val = iter2.currentValue();
      assertEquals(hm.get(new String(keyc)), val);
      iter2.setValue(val*100);
      assertEquals(val*100, (int)cm.get(keyc));
      n++;
    }
    assertEquals(hm.size(), n);

    cm.clear();
    assertEquals(0, cm.size());
    assertTrue(cm.isEmpty());
  }

  
  // performance test vs HashMap<String,Object>
  // HashMap will have an edge because we are testing with
  // non-dynamically created keys and String caches hashCode
  public static void main(String[] args) {
    int a=0;
    String impl = args[a++].intern();          // hash OR chars OR char
    int iter1 = Integer.parseInt(args[a++]);   // iterations of put()
    int iter2 = Integer.parseInt(args[a++]);   // iterations of get()

    int ret=0;
    long start = System.currentTimeMillis();
    String[] stopwords = StopAnalyzer.ENGLISH_STOP_WORDS;
    // words = "this is a different test to see what is really going on here... I hope it works well but I'm not sure it will".split(" ");
    char[][] stopwordschars = new char[stopwords.length][];
    for (int i=0; i<stopwords.length; i++) {
      stopwordschars[i] = stopwords[i].toCharArray();
    }

    String[] testwords = "now is the time for all good men to come to the aid of their country".split(" ");
    // testwords = "this is a different test to see what is really going on here... I hope it works well but I'm not sure it will".split(" ");
    char[][] testwordchars = new char[testwords.length][];

    for (int i=0; i<testwordchars.length; i++) {
      testwordchars[i] = testwords[i].toCharArray();
    }

    HashMap<String,Integer> hm=null;
    CharArrayMap<Integer> cm=null;

    if (impl=="hash") {
      for (int i=0; i<iter1; i++) {

        hm = new HashMap<String,Integer>();
        int v=0;
        for (String word : stopwords) {
          hm.put(word, ++v);
        }
        ret += hm.size();
      }
    } else if (impl=="chars") {
      for (int i=0; i<iter1; i++) {
        cm = new CharArrayMap<Integer>(2,false);
        int v=0;
        for (String s : stopwords) {
          cm.put(s,++v);
        }
        ret += cm.size();
      }
    } else if (impl=="char") {
      for (int i=0; i<iter1; i++) {
        cm = new CharArrayMap<Integer>(2,false);
        int v=0;
        for (char[] s : stopwordschars) {
          cm.put(s,++v);
        }
        ret += cm.size();
      }
    }


    if (impl=="hash") {
      for (int i=0; i<iter2; i++) {
        for (String word : testwords) {
          Integer v = hm.get(word);
          ret += v==null ? 0 : v;
        }
      }
    } else if (impl=="chars") {
      for (int i=0; i<iter2; i++) {
        for (String word : testwords) {
          Integer v = cm.get(word);
          ret += v==null ? 0 : v;
        }
      }
    } else if (impl=="char") {
      for (int i=0; i<iter2; i++) {
        for (char[] word : testwordchars) {
          Integer v = cm.get(word);
          ret += v==null ? 0 : v;
        }
      }
    }

    long end = System.currentTimeMillis();

    System.out.println("result=" + ret);
    System.out.println("time=" +(end-start));
  }

}

