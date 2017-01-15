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
package org.apache.solr.morphlines.solr;

import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;

/**
 * A command that uses the embedded Solr/Lucene Analyzer library to generate tokens from a text
 * string, without sending data to a Solr server.
 */
public final class TokenizeTextBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("tokenizeText");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new TokenizeText(this, config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class TokenizeText extends AbstractCommand {
    
    private final String inputFieldName;
    private final String outputFieldName;
    private final Analyzer analyzer;
    private final CharTermAttribute token; // cached
    private final ReusableStringReader reader = new ReusableStringReader(); // cached
    
    public TokenizeText(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      this.inputFieldName = getConfigs().getString(config, "inputField");
      this.outputFieldName = getConfigs().getString(config, "outputField");      
      String solrFieldType = getConfigs().getString(config, "solrFieldType");      
      Config solrLocatorConfig = getConfigs().getConfig(config, "solrLocator");
      SolrLocator locator = new SolrLocator(solrLocatorConfig, context);
      LOG.debug("solrLocator: {}", locator);
      IndexSchema schema = locator.getIndexSchema();
      FieldType fieldType = schema.getFieldTypeByName(solrFieldType);
      if (fieldType == null) {
        throw new MorphlineCompilationException("Missing Solr field type in schema.xml for name: " + solrFieldType, config);
      }
      this.analyzer = Objects.requireNonNull(fieldType.getIndexAnalyzer());
      // register CharTermAttribute for later (implicit) reuse
      this.token = Objects.requireNonNull(analyzer.tokenStream("content", reader).addAttribute(CharTermAttribute.class));
      validateArguments();
    }

    @Override
    protected boolean doProcess(Record record) {
      try {
        List outputValues = record.get(outputFieldName);
        for (Object value : record.get(inputFieldName)) {
          reader.setValue(value.toString());
          TokenStream tokenStream = analyzer.tokenStream("content", reader);
          tokenStream.reset();
          while (tokenStream.incrementToken()) {
            if (token.length() > 0) { // incrementToken() updates the token!
              String tokenStr = new String(token.buffer(), 0, token.length());
              outputValues.add(tokenStr);
            }
          }
          tokenStream.end();
          tokenStream.close();
        }
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      }
      
      // pass record to next command in chain:
      return super.doProcess(record);
    }

  }
  
  private static final class ReusableStringReader extends Reader {
    private int pos = 0, size = 0;
    private String s = null;
    
    void setValue(String s) {
      this.s = s;
      this.size = s.length();
      this.pos = 0;
    }
    
    @Override
    public int read() {
      if (pos < size) {
        return s.charAt(pos++);
      } else {
        s = null;
        return -1;
      }
    }
    
    @Override
    public int read(char[] c, int off, int len) {
      if (pos < size) {
        len = Math.min(len, size-pos);
        s.getChars(pos, pos+len, c, off);
        pos += len;
        return len;
      } else {
        s = null;
        return -1;
      }
    }
    
    @Override
    public void close() {
      pos = size; // this prevents NPE when reading after close!
      s = null;
    }
  }
}
