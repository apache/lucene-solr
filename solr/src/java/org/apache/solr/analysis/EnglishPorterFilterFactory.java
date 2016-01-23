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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.tartarus.snowball.SnowballProgram;

import java.io.IOException;
import java.io.File;
import java.util.List;

/**
 * @version $Id$
 *
 * @deprecated Use SnowballPorterFilterFactory with language="English" instead
 */
public class EnglishPorterFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  public static final String PROTECTED_TOKENS = "protected";

  public void inform(ResourceLoader loader) {
    String wordFiles = args.get(PROTECTED_TOKENS);
    if (wordFiles != null) {
      try {
        File protectedWordFiles = new File(wordFiles);
        if (protectedWordFiles.exists()) {
          List<String> wlist = loader.getLines(wordFiles);
          //This cast is safe in Lucene
          protectedWords = new CharArraySet(wlist, false);//No need to go through StopFilter as before, since it just uses a List internally
        } else  {
          List<String> files = StrUtils.splitFileNames(wordFiles);
          for (String file : files) {
            List<String> wlist = loader.getLines(file.trim());
            if (protectedWords == null)
              protectedWords = new CharArraySet(wlist, false);
            else
              protectedWords.addAll(wlist);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private CharArraySet protectedWords = null;

  public EnglishPorterFilter create(TokenStream input) {
    return new EnglishPorterFilter(input, protectedWords);
  }

}


/**
 * English Porter2 filter that doesn't use reflection to
 * adapt lucene to the snowball stemmer code.
 */
@Deprecated
class EnglishPorterFilter extends SnowballPorterFilter {
  public EnglishPorterFilter(TokenStream source, CharArraySet protWords) {
    super(source, new org.tartarus.snowball.ext.EnglishStemmer(), protWords);
  }
}
