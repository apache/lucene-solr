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
package org.apache.solr.search.similarities;

import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Version;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * <p>
 * <code>SimilarityFactory</code> that returns a global {@link PerFieldSimilarityWrapper}
 * that delegates to the field type, if it's configured.  For field type's that
 * do not have a <code>Similarity</code> explicitly configured, the global <code>Similarity</code> 
 * will use per fieldtype defaults -- either based on an explicitly configured 
 * <code>defaultSimFromFieldType</code> a sensible default depending on the {@link Version} 
 * matching configured:
 * </p>
 * <ul>
 *  <li><code>luceneMatchVersion &lt; 6.0</code> = {@link ClassicSimilarity}</li>
 *  <li><code>luceneMatchVersion &gt;= 6.0</code> = {@link BM25Similarity}</li>
 * </ul>
 * <p>
 * The <code>defaultSimFromFieldType</code> option accepts the name of any fieldtype, and uses 
 * whatever <code>Similarity</code> is explicitly configured for that fieldType as thedefault for 
 * all other field types.  For example:
 * </p>
 * <pre class="prettyprint">
 *   &lt;similarity class="solr.SchemaSimilarityFactory" &gt;
 *     &lt;str name="defaultSimFromFieldType"&gt;type-using-custom-dfr&lt;/str&gt;
 *   &lt;/similarity&gt;
 *   ...
 *   &lt;fieldType name="type-using-custom-dfr" class="solr.TextField"&gt;
 *     ...
 *     &lt;similarity class="solr.DFRSimilarityFactory"&gt;
 *       &lt;str name="basicModel"&gt;I(F)&lt;/str&gt;
 *       &lt;str name="afterEffect"&gt;B&lt;/str&gt;
 *       &lt;str name="normalization"&gt;H3&lt;/str&gt;
 *       &lt;float name="mu"&gt;900&lt;/float&gt;
 *     &lt;/similarity&gt;
 *   &lt;/fieldType&gt;
 * </pre>
 * <p>
 * In the example above, any fieldtypes that do not define their own <code>&lt;/similarity/&gt;</code> 
 * will use the <code>Similarity</code> configured for the <code>type-using-custom-dfr</code>.
 * </p>
 * 
 * <p>
 * <b>NOTE:</b> Users should be aware that even when this factory uses a single default 
 * <code>Similarity</code> for some or all fields in a Query, the behavior can be inconsistent 
 * with the behavior of explicitly configuring that same <code>Similarity</code> globally, because 
 * of differences in how some multi-field / multi-clause behavior is defined in 
 * <code>PerFieldSimilarityWrapper</code>.
 * </p>
 *
 * @see FieldType#getSimilarity
 */
public class SchemaSimilarityFactory extends SimilarityFactory implements SolrCoreAware {

  private static final String INIT_OPT = "defaultSimFromFieldType";
  
  private String defaultSimFromFieldType; // set by init, if null use sensible implicit default
  
  private volatile SolrCore core; // set by inform(SolrCore)
  private volatile Similarity similarity; // lazy instantiated

  @Override
  public void inform(SolrCore core) {
    this.core = core;
  }
  
  @Override
  public void init(SolrParams args) {
    defaultSimFromFieldType = args.get(INIT_OPT, null);
    super.init(args);
  }

  @Override
  public Similarity getSimilarity() {
    if (null == core) {
      throw new IllegalStateException("SchemaSimilarityFactory can not be used until SolrCoreAware.inform has been called");
    }
    if (null == similarity) {
      // Need to instantiate lazily, can't do this in inform(SolrCore) because of chicken/egg
      // circular initialization hell with core.getLatestSchema() to lookup defaultSimFromFieldType
      
      Similarity defaultSim = null;
      if (null == defaultSimFromFieldType) {
        // nothing configured, choose a sensible implicit default...
        defaultSim = this.core.getSolrConfig().luceneMatchVersion.onOrAfter(Version.LUCENE_6_0_0)
          ? new BM25Similarity()
          : new ClassicSimilarity();
      } else {
        FieldType defSimFT = core.getLatestSchema().getFieldTypeByName(defaultSimFromFieldType);
        if (null == defSimFT) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
                                  "SchemaSimilarityFactory configured with " + INIT_OPT + "='" +
                                  defaultSimFromFieldType + "' but that <fieldType> does not exist");
                                  
        }
        defaultSim = defSimFT.getSimilarity();
        if (null == defaultSim) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
                                  "SchemaSimilarityFactory configured with " + INIT_OPT + "='" + 
                                  defaultSimFromFieldType +
                                  "' but that <fieldType> does not define a <similarity>");
        }
      }
      similarity = new SchemaSimilarity(defaultSim);
    }
    return similarity;
  }
  
  private class SchemaSimilarity extends PerFieldSimilarityWrapper {
    private Similarity defaultSimilarity;

    public SchemaSimilarity(Similarity defaultSimilarity) {
      this.defaultSimilarity = defaultSimilarity;
    }

    @Override
    public Similarity get(String name) {
      FieldType fieldType = core.getLatestSchema().getFieldTypeNoEx(name);
      if (fieldType == null) {
        return defaultSimilarity;
      } else {
        Similarity similarity = fieldType.getSimilarity();
        return similarity == null ? defaultSimilarity : similarity;
      }
    }

    @Override
    public String toString() {
      return "SchemaSimilarity. Default: " + ((get("") == null) ? "null" : get("").toString());
    }
  }
}
