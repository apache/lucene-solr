package org.apache.solr.analysis;

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

import java.io.Reader;
import java.io.StringReader;
import org.apache.lucene.analysis.Tokenizer;

/**
 * A few tests based on  org.apache.lucene.analysis.TestUAX29Tokenizer;
 */

public class TestUAX29TokenizerFactory extends BaseTokenTestCase {
  /**
   * Test UAX29TokenizerFactory
   */
  public void testUAX29Tokenizer() throws Exception {
    Reader reader = new StringReader("Wha\u0301t's this thing do?");
    UAX29TokenizerFactory factory = new UAX29TokenizerFactory();
    factory.init(DEFAULT_VERSION_PARAM);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"Wha\u0301t's", "this", "thing", "do" });
  }
  
  public void testArabic() throws Exception {
    Reader reader = new StringReader("الفيلم الوثائقي الأول عن ويكيبيديا يسمى \"الحقيقة بالأرقام: قصة ويكيبيديا\" (بالإنجليزية: Truth in Numbers: The Wikipedia Story)، سيتم إطلاقه في 2008.");
    UAX29TokenizerFactory factory = new UAX29TokenizerFactory();
    factory.init(DEFAULT_VERSION_PARAM);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"الفيلم", "الوثائقي", "الأول", "عن", "ويكيبيديا", "يسمى", "الحقيقة", "بالأرقام", "قصة", "ويكيبيديا",
        "بالإنجليزية", "Truth", "in", "Numbers", "The", "Wikipedia", "Story", "سيتم", "إطلاقه", "في", "2008"  });
  }
  
  public void testChinese() throws Exception {
    Reader reader = new StringReader("我是中国人。 １２３４ Ｔｅｓｔｓ ");
    UAX29TokenizerFactory factory = new UAX29TokenizerFactory();
    factory.init(DEFAULT_VERSION_PARAM);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"我", "是", "中", "国", "人", "１２３４", "Ｔｅｓｔｓ"});
  }
  public void testKorean() throws Exception {
    Reader reader = new StringReader("안녕하세요 한글입니다");
    UAX29TokenizerFactory factory = new UAX29TokenizerFactory();
    factory.init(DEFAULT_VERSION_PARAM);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"안녕하세요", "한글입니다"});
  }
    
  public void testHyphen() throws Exception {
    Reader reader = new StringReader("some-dashed-phrase");
    UAX29TokenizerFactory factory = new UAX29TokenizerFactory();
    factory.init(DEFAULT_VERSION_PARAM);
    Tokenizer stream = factory.create(reader);
    assertTokenStreamContents(stream, 
        new String[] {"some", "dashed", "phrase"});
  }

}
    
  
  
  
