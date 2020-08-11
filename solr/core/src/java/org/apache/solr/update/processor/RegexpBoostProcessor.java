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
package org.apache.solr.update.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A processor which will match content of "inputField" against regular expressions
 * found in "boostFilename", and if it matches will return the corresponding boost
 * value from the file and output this to "boostField" as a double value.
 * If more than one pattern matches, the boosts from each are multiplied.
 * <p>
 * A typical use case may be to match a URL against patterns to boost or deboost
 * web documents based on the URL itself:
 * <pre>
 * # Format of each line: &lt;pattern&gt;&lt;TAB&gt;&lt;boost&gt;
 * # Example:
 * https?://my.domain.com/temp.*  0.2
 * </pre>
 * <p>
 * Both inputField, boostField and boostFilename are mandatory parameters.
 */
public class RegexpBoostProcessor extends UpdateRequestProcessor {

  protected static final String INPUT_FIELD_PARAM = "inputField";
  protected static final String BOOST_FIELD_PARAM = "boostField";
  protected static final String BOOST_FILENAME_PARAM = "boostFilename";
  private static final String DEFAULT_INPUT_FIELDNAME = "url";
  private static final String DEFAULT_BOOST_FIELDNAME = "urlboost";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean enabled = true;
  private String inputFieldname = DEFAULT_INPUT_FIELDNAME;
  private String boostFieldname = DEFAULT_BOOST_FIELDNAME;
  private String boostFilename;
  private List<BoostEntry> boostEntries = new ArrayList<>();
  private static final String BOOST_ENTRIES_CACHE_KEY = "boost-entries";

  RegexpBoostProcessor(SolrParams parameters,
                       SolrQueryRequest request,
                       SolrQueryResponse response,
                       UpdateRequestProcessor nextProcessor,
                       final Map<Object, Object> sharedObjectCache) {
    super(nextProcessor);
    this.initParameters(parameters);

    if (this.boostFilename == null) {
      log.warn("Null boost filename.  Disabling processor.");
      setEnabled(false);
    }

    if (!isEnabled()) {
      return;
    }

    try {
      synchronized (sharedObjectCache) {
        @SuppressWarnings({"unchecked"})
        List<BoostEntry> cachedBoostEntries =
            (List<BoostEntry>) sharedObjectCache.get(BOOST_ENTRIES_CACHE_KEY);

        if (cachedBoostEntries == null) {
          log.debug("No pre-cached boost entry list found, initializing new");
          InputStream is = request.getCore().getResourceLoader().openResource(boostFilename);
          cachedBoostEntries = initBoostEntries(is);
          sharedObjectCache.put(BOOST_ENTRIES_CACHE_KEY, cachedBoostEntries);
        } else {
          if (log.isDebugEnabled()) {
            log.debug("Using cached boost entry list with {} elements", cachedBoostEntries.size());
          }
        }

        this.boostEntries = cachedBoostEntries;
      }
    } catch (IOException ioe) {
      log.warn("IOException while initializing boost entries from file {}", this.boostFilename, ioe);
    }
  }

  private void initParameters(SolrParams parameters) {
    if (parameters != null) {
      this.setEnabled(parameters.getBool("enabled", true));
      this.inputFieldname = parameters.get(INPUT_FIELD_PARAM, DEFAULT_INPUT_FIELDNAME);
      this.boostFieldname = parameters.get(BOOST_FIELD_PARAM, DEFAULT_BOOST_FIELDNAME);
      this.boostFilename = parameters.get(BOOST_FILENAME_PARAM);
    }
  }

  private List<BoostEntry> initBoostEntries(InputStream is) throws IOException {
    List<BoostEntry> newBoostEntries = new ArrayList<>();
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    try {
      String line = null;
      while ((line = reader.readLine()) != null) {
        // Remove comments
        line = line.replaceAll("\\s+#.*$", "");
        line = line.replaceAll("^#.*$", "");

        // Skip empty lines or comment lines
        if (line.trim().length() == 0) {
          continue;
        }

        String[] fields = line.split("\\s+");

        if (fields.length == 2) {
          String regexp = fields[0];
          String boost = fields[1];
          newBoostEntries.add(new BoostEntry(Pattern.compile(regexp), Double.parseDouble(boost)));
          log.debug("Read regexp {} with boost {}", regexp, boost);
        } else {
          log.warn("Malformed config input line: {} (expected 2 fields, got {} fields).  Skipping entry.", line, fields.length);
          continue;
        }
      }
    } finally {
      IOUtils.closeQuietly(reader);
    }

    return newBoostEntries;
  }

  @Override
  public void processAdd(AddUpdateCommand command) throws IOException {
    if (isEnabled()) {
      processBoost(command);
    }
    super.processAdd(command);
  }

  public void processBoost(AddUpdateCommand command) {
    SolrInputDocument document = command.getSolrInputDocument();
    if (document.containsKey(inputFieldname)) {
      String value = (String) document.getFieldValue(inputFieldname);
      double boost = 1.0f;
      for (BoostEntry boostEntry : boostEntries) {
        if (boostEntry.getPattern().matcher(value).matches()) {
          if (log.isDebugEnabled()) {
            log.debug("Pattern match {} for {}", boostEntry.getPattern().pattern(), value);
          }
          boost = (boostEntry.getBoost() * 1000) * (boost * 1000) / 1000000;
        }
      }
      document.setField(boostFieldname, boost);

      if (log.isDebugEnabled()) {
        log.debug("Value {}, applied to field {}", boost, boostFieldname);
      }
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  private static class BoostEntry {

    private Pattern pattern;
    private double boost;

    public BoostEntry(Pattern pattern, double d) {
      this.pattern = pattern;
      this.boost = d;
    }

    public Pattern getPattern() {
      return pattern;
    }

    public double getBoost() {
      return boost;
    }
  }
}
