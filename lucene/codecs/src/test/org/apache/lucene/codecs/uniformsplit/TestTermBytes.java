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

package org.apache.lucene.codecs.uniformsplit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests {@link TermBytes}.
 */
public class TestTermBytes extends LuceneTestCase {

  public void testMDPA() {
    validateExpectedMDP(new String[][]{
        {"aa", "a"},
        {"abbreviator", "ab"},
        {"abidingly", "abi"},
        {"aboiteaus", "abo"},
        {"abranchiates", "abr"},
        {"absentminded", "abs"},
    });
  }

  public void testIncrementalA() {
    validateExpectedSuffix(new String[][]{
        {"aa", "0aa"},
        {"abbreviator", "1bbreviator"},
        {"abidingly", "2idingly"},
        {"aboiteaus", "2oiteaus"},
        {"abranchiates", "2ranchiates"},
        {"absentminded", "2sentminded"},
        {"rodriguez", "0rodriguez"},
        {"romero", "2mero"},
    });
  }

  public void testMDPMIX2() {
    validateExpectedMDP(new String[][]{
        {"abaco", "a"},
        {"amigo", "am"},
        {"bloom", "b"},
        {"break", "br"},
        {"can", "c"},
        {"car", "car"},
        {"carmagedon", "carm"},
        {"danger", "d"},
        {"lala", "l"},
        {"literature", "li"},
        {"lucene", "lu"},
        {"nature", "n"},
        {"naval", "nav"},
        {"rico", "r"},
        {"weird", "w"},
        {"zoo", "z"},
    });
  }

  public void testMDP() {
    validateExpectedMDP(new String[][]{
        {"abaco", "a"},
        {"amigo", "am"},
        {"arco", "ar"},
        {"bloom", "b"},
        {"frien", "f"},
        {"frienchies", "frienc"},
        {"friend", "friend"},
        {"friendalan", "frienda"},
        {"friende", "friende"},
    });
  }

  public void testIncremental() {
    validateExpectedSuffix(new String[][]{
        {"abaco", "0abaco"},
        {"amigo", "1migo"},
        {"arco", "1rco"},
        {"bloom", "0bloom"},
        {"frien", "0frien"},
        {"frienchies", "5chies"},
        {"friend", "5d"},
        {"friendalan", "6alan"},
        {"friende", "6e"},
    });
  }

  public void testIncrementalSimple() {
    validateExpectedSuffix(new String[][]{
        {"abaco", "0abaco"},
        {"rodriguez", "0rodriguez"},
        {"roma", "2ma"},
        {"romero", "3ero"},
    });
  }

  public void testMDPSimple() {
    validateExpectedMDP(new String[][]{
        {"abaco", "a"},
        {"rodriguez", "r"},
        {"romero", "rom"},
    });
  }

  public void testMDPMIX() {
    validateExpectedMDP(new String[][]{
        {"aaab", "a"},
        {"arco", "ar"},
        {"busqueda", "b"},
        {"trabajo", "t"},
        {"zufix", "z"},
        {"zzfix", "zz"},
    });
  }
  
  private void validateExpectedSuffix(String[][] vocab) {
    Map<String, String> vocabMap = toMap(vocab);
    validateExpectedSuffix(vocabMap);
    validateIncrementalDecoding(vocabMap);
  }

  private void validateExpectedSuffix(Map<String, String> vocab) {
    List<BytesRef> src = vocab.keySet().stream().sorted().map(BytesRef::new).collect(Collectors.toList());
    List<TermBytes> output = compressPrefixes(src);
    validateMapList(vocab,
        src.stream().map(BytesRef::utf8ToString).collect(Collectors.toList()),
        output.stream().map(e -> e.getSuffixOffset() + createSuffixBytes(e).utf8ToString()).collect(Collectors.toList()));
  }

  private BytesRef createSuffixBytes(TermBytes termBytes) {
    return new BytesRef(termBytes.getTerm().bytes, termBytes.getSuffixOffset(), termBytes.getSuffixLength());
  }

  private void validateExpectedMDP(String[][] vocab) {
    Map<String, String> vocabMap = toMap(vocab);
    validateExpectedMDP(vocabMap);
    validateIncrementalDecoding(vocabMap);
  }

  private void validateExpectedMDP(Map<String, String> vocab) {
    List<BytesRef> src = vocab.keySet().stream().sorted().map(BytesRef::new).collect(Collectors.toList());
    List<TermBytes> output = compressPrefixes(src);
    validateMapList(vocab,
        src.stream().map(BytesRef::utf8ToString).collect(Collectors.toList()),
        output.stream().map(e -> new BytesRef(e.getTerm().bytes, 0, e.getMdpLength()).utf8ToString())
            .collect(Collectors.toList()));
  }

  private void validateIncrementalDecoding(Map<String, String> vocab) {
    BytesRef previous = new BytesRef(80);
    List<BytesRef> src = vocab.keySet().stream().sorted().map(BytesRef::new).collect(Collectors.toList());
    List<TermBytes> output = compressPrefixes(src);

    for (int i = 0; i < src.size(); i++) {
      copyBytes(BytesRef.deepCopyOf(createSuffixBytes(output.get(i))), previous, output.get(i).getSuffixOffset());
      assertEquals("Error in line " + i, src.get(i).utf8ToString(), previous.utf8ToString());
    }
  }

  private void validateMapList(Map<String, String> expectedMap, List<String> src, List<String> result) {
    for (int i = 0; i < src.size(); i++) {
      assertEquals("Error in line " + i, expectedMap.get(src.get(i)), result.get(i));
    }
  }

  private static List<TermBytes> compressPrefixes(List<BytesRef> vocab) {
    List<TermBytes> termBytes = new ArrayList<>(vocab.size());
    BytesRef last = null;
    TermBytes term;
    int mdp;
    for (BytesRef current : vocab) {
      mdp = TermBytes.computeMdpLength(last, current);
      term = new TermBytes(mdp, current);
      termBytes.add(term);
      last = current;
    }
    return termBytes;
  }

  private static void copyBytes(BytesRef source, BytesRef target, int targetOffset) {
    assert target.offset == 0;
    assert source.offset == 0;
    int newLength = targetOffset + source.length;
    if (newLength > target.bytes.length) {
      byte[] copy = new byte[newLength];
      System.arraycopy(target.bytes, 0, copy, 0, targetOffset);
      target.bytes = copy;
    }
    target.length = newLength;
    System.arraycopy(source.bytes, 0, target.bytes, targetOffset, source.length);
  }

  private static Map<String, String> toMap(String[][] src) {
    assert src.length > 0 : "insert at least one row";
    assert src[0].length == 2 : "two columns are mandatory";
    return Arrays.stream(src).collect(Collectors.toMap(kv -> kv[0], kv -> kv[1]));
  }
}
