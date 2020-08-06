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
package org.apache.solr.search;

import java.util.Locale;
import java.util.Objects;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.RankField;
import org.apache.solr.schema.SchemaField;
/**
 * {@code RankQParserPlugin} can be used to introduce document-depending scoring factors to ranking.
 * While this {@code QParser} delivers a (subset of) functionality already available via {@link FunctionQParser},
 * the benefit is that {@code RankQParserPlugin} can be used in combination with the {@code minExactCount} to
 * use BlockMax-WAND algorithm (skip non-competitive documents) to provide faster responses. 
 * 
 *  @see RankField
 * 
 * @lucene.experimental
 * @since 8.6
 */
public class RankQParserPlugin extends QParserPlugin {
  
  public static final String NAME = "rank";
  public static final String FIELD = "f";
  public static final String FUNCTION = "function";
  public static final String WEIGHT = "weight";
  public static final String PIVOT = "pivot";
  public static final String SCALING_FACTOR = "scalingFactor";
  public static final String EXPONENT = "exponent";
  
  private final static FeatureFieldFunction DEFAULT_FUNCTION = FeatureFieldFunction.SATU;
  
  private enum FeatureFieldFunction {
    SATU {
      @Override
      public Query createQuery(String fieldName, SolrParams params) throws SyntaxError {
        Float weight = params.getFloat(WEIGHT);
        Float pivot = params.getFloat(PIVOT);
        if (pivot == null && (weight == null || Float.compare(weight.floatValue(), 1f) == 0)) {
          // No IAE expected in this case
          return FeatureField.newSaturationQuery(RankField.INTERNAL_RANK_FIELD_NAME, fieldName);
        }
        if (pivot == null) {
          throw new SyntaxError("A pivot value needs to be provided if the weight is not 1 on \"satu\" function");
        }
        if (weight == null) {
          weight = Float.valueOf(1);
        }
        try {
          return FeatureField.newSaturationQuery(RankField.INTERNAL_RANK_FIELD_NAME, fieldName, weight, pivot);
        } catch (IllegalArgumentException iae) {
          throw new SyntaxError(iae.getMessage());
        }
      }
    },
    LOG {
      @Override
      public Query createQuery(String fieldName, SolrParams params) throws SyntaxError {
        float weight = params.getFloat(WEIGHT, 1f);
        float scalingFactor = params.getFloat(SCALING_FACTOR, 1f);
        try {
          return FeatureField.newLogQuery(RankField.INTERNAL_RANK_FIELD_NAME, fieldName, weight, scalingFactor);
        } catch (IllegalArgumentException iae) {
          throw new SyntaxError(iae.getMessage());
        }
      }
    },
    SIGM {
      @Override
      public Query createQuery(String fieldName, SolrParams params) throws SyntaxError {
        float weight = params.getFloat(WEIGHT, 1f);
        Float pivot = params.getFloat(PIVOT);
        if (pivot == null) {
          throw new SyntaxError("A pivot value needs to be provided when using \"sigm\" function");
        }
        Float exponent = params.getFloat(EXPONENT);
        if (exponent == null) {
          throw new SyntaxError("An exponent value needs to be provided when using \"sigm\" function");
        }
        try {
          return FeatureField.newSigmoidQuery(RankField.INTERNAL_RANK_FIELD_NAME, fieldName, weight, pivot, exponent);
        } catch (IllegalArgumentException iae) {
          throw new SyntaxError(iae.getMessage());
        }
      }
    };
    
    public abstract Query createQuery(String fieldName, SolrParams params) throws SyntaxError;
    
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    Objects.requireNonNull(localParams, "LocalParams String can't be null");
    Objects.requireNonNull(req, "SolrQueryRequest can't be null");
    return new RankQParser(qstr, localParams, params, req);
  }
  
  public static class RankQParser extends QParser {
    
    private final String field;

    public RankQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
      this.field = localParams.get(FIELD);
    }

    @Override
    public Query parse() throws SyntaxError {
      if (this.field == null || this.field.isEmpty()) {
        throw new SyntaxError("Field can't be empty in rank queries");
      }
      SchemaField schemaField = req.getSchema().getFieldOrNull(field);
      if (schemaField == null) {
        throw new SyntaxError("Field \"" + this.field + "\" not found");
      }
      if (!(schemaField.getType() instanceof RankField)) {
        throw new SyntaxError("Field \"" + this.field + "\" is not a RankField");
      }
      return getFeatureFieldFunction(localParams.get(FUNCTION))
          .createQuery(field, localParams);
    }

    private FeatureFieldFunction getFeatureFieldFunction(String function) throws SyntaxError {
      FeatureFieldFunction f = null;
      if (function == null || function.isEmpty()) {
        f = DEFAULT_FUNCTION;
      } else {
        try {
          f = FeatureFieldFunction.valueOf(function.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException iae) {
          throw new SyntaxError("Unknown function in rank query: \"" + function + "\"");
        }
      }
      return f;
    }
    
  }

}
