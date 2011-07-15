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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilter;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.apache.lucene.analysis.payloads.FloatEncoder;
import org.apache.lucene.analysis.payloads.IntegerEncoder;
import org.apache.lucene.analysis.payloads.IdentityEncoder;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.plugin.ResourceLoaderAware;

import java.util.Map;


/**
 *
 * Factory for {@link DelimitedPayloadTokenFilter}.
 * <pre class="prettyprint" >
 * &lt;fieldType name="text_dlmtd" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *     &lt;filter class="solr.DelimitedPayloadTokenFilterFactory" encoder="float" delimiter="|"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * 
 */
public class DelimitedPayloadTokenFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  public static final String ENCODER_ATTR = "encoder";
  public static final String DELIMITER_ATTR = "delimiter";

  private PayloadEncoder encoder;
  private char delimiter = '|';

  public DelimitedPayloadTokenFilter create(TokenStream input) {
    return new DelimitedPayloadTokenFilter(input, delimiter, encoder);
  }

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
  }

  public void inform(ResourceLoader loader) {
    String encoderClass = args.get(ENCODER_ATTR);
    if (encoderClass.equals("float")){
      encoder = new FloatEncoder();
    } else if (encoderClass.equals("integer")){
      encoder = new IntegerEncoder();
    } else if (encoderClass.equals("identity")){
      encoder = new IdentityEncoder();
    } else {
      encoder = (PayloadEncoder) loader.newInstance(encoderClass);
    }

    String delim = args.get(DELIMITER_ATTR);
    if (delim != null){
      if (delim.length() == 1) {
        delimiter = delim.charAt(0);
      } else{
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Delimiter must be one character only");
      }
    }
  }
}