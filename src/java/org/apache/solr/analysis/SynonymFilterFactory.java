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

import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.plugin.ResourceLoaderAware;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @version $Id$
 */
public class SynonymFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  
  public void inform(ResourceLoader loader) {
    String synonyms = args.get("synonyms");

    boolean ignoreCase = getBoolean("ignoreCase", false);
    boolean expand = getBoolean("expand", true);

    if (synonyms != null) {
      List<String> wlist=null;
      try {
        wlist = loader.getLines(synonyms);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      synMap = new SynonymMap(ignoreCase);
      parseRules(wlist, synMap, "=>", ",", expand);
      if (wlist.size()<=20) {
        SolrCore.log.fine("SynonymMap "+synonyms +":"+synMap);
      }
    }
  }

  private SynonymMap synMap;

  private static void parseRules(List<String> rules, SynonymMap map, String mappingSep, String synSep, boolean expansion) {
    int count=0;
    for (String rule : rules) {
      // To use regexes, we need an expression that specifies an odd number of chars.
      // This can't really be done with string.split(), and since we need to
      // do unescaping at some point anyway, we wouldn't be saving any effort
      // by using regexes.

      List<String> mapping = StrUtils.splitSmart(rule, mappingSep, false);

      List<List<String>> source;
      List<List<String>> target;

      if (mapping.size() > 2) {
        throw new RuntimeException("Invalid Synonym Rule:" + rule);
      } else if (mapping.size()==2) {
        source = getSynList(mapping.get(0), synSep);
        target = getSynList(mapping.get(1), synSep);
      } else {
        source = getSynList(mapping.get(0), synSep);
        if (expansion) {
          // expand to all arguments
          target = source;
        } else {
          // reduce to first argument
          target = new ArrayList<List<String>>(1);
          target.add(source.get(0));
        }
      }

      boolean includeOrig=false;
      for (List<String> fromToks : source) {
        count++;
        for (List<String> toToks : target) {
          map.add(fromToks,
                  SynonymMap.makeTokens(toToks),
                  includeOrig,
                  true
          );
        }
      }
    }
  }

  // a , b c , d e f => [[a],[b,c],[d,e,f]]
  private static List<List<String>> getSynList(String str, String separator) {
    List<String> strList = StrUtils.splitSmart(str, separator, false);
    // now split on whitespace to get a list of token strings
    List<List<String>> synList = new ArrayList<List<String>>();
    for (String toks : strList) {
      List<String> tokList = StrUtils.splitWS(toks, true);
      synList.add(tokList);
    }
    return synList;
  }


  public SynonymFilter create(TokenStream input) {
    return new SynonymFilter(input,synMap);
  }


}
