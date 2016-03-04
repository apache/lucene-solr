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
package org.apache.solr.schema;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Map;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.SolrCache;

/** A Solr Spatial FieldType based on {@link CompositeSpatialStrategy}.
 * @lucene.experimental */
public class RptWithGeometrySpatialField extends AbstractSpatialFieldType<CompositeSpatialStrategy> {

  public static final String DEFAULT_DIST_ERR_PCT = "0.15";

  private SpatialRecursivePrefixTreeFieldType rptFieldType;
  private SolrCore core;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    // Do NOT call super.init(); instead we delegate to an RPT field. Admittedly this is error prone.

    //TODO Move this check to a call from AbstractSpatialFieldType.createFields() so the type can declare
    // if it supports multi-valued or not. It's insufficient here; we can't see if you set multiValued on the field.
    if (isMultiValued()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Not capable of multiValued: " + getTypeName());
    }

    // Choose a better default distErrPct if not configured
    if (args.containsKey(SpatialArgsParser.DIST_ERR_PCT) == false) {
      args.put(SpatialArgsParser.DIST_ERR_PCT, DEFAULT_DIST_ERR_PCT);
    }

    rptFieldType = new SpatialRecursivePrefixTreeFieldType();
    rptFieldType.setTypeName(getTypeName());
    rptFieldType.properties = properties;
    rptFieldType.init(schema, args);
    rptFieldType.argsParser = argsParser = newSpatialArgsParser();
    this.ctx = rptFieldType.ctx;
    this.distanceUnits = rptFieldType.distanceUnits;
  }

  @Override
  protected CompositeSpatialStrategy newSpatialStrategy(String fieldName) {
    // We use the same field name for both sub-strategies knowing there will be no conflict for these two

    RecursivePrefixTreeStrategy rptStrategy = rptFieldType.newSpatialStrategy(fieldName);

    SerializedDVStrategy geomStrategy = new CachingSerializedDVStrategy(ctx, fieldName);

    return new CompositeSpatialStrategy(fieldName, rptStrategy, geomStrategy);
  }

  @Override
  public Analyzer getQueryAnalyzer() {
    return rptFieldType.getQueryAnalyzer();
  }

  @Override
  public Analyzer getIndexAnalyzer() {
    return rptFieldType.getIndexAnalyzer();
  }

  // Most of the complexity of this field type is below, which is all about caching the shapes in a SolrCache

  private static class CachingSerializedDVStrategy extends SerializedDVStrategy {
    public CachingSerializedDVStrategy(SpatialContext ctx, String fieldName) {
      super(ctx, fieldName);
    }

    @Override
    public ValueSource makeShapeValueSource() {
      return new CachingShapeValuesource(super.makeShapeValueSource(), getFieldName());
    }
  }

  private static class CachingShapeValuesource extends ValueSource {

    private final ValueSource targetValueSource;
    private final String fieldName;

    private CachingShapeValuesource(ValueSource targetValueSource, String fieldName) {
      this.targetValueSource = targetValueSource;
      this.fieldName = fieldName;
    }

    @Override
    public String description() {
      return "cache(" + targetValueSource.description() + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CachingShapeValuesource that = (CachingShapeValuesource) o;

      if (!targetValueSource.equals(that.targetValueSource)) return false;
      return fieldName.equals(that.fieldName);

    }

    @Override
    public int hashCode() {
      int result = targetValueSource.hashCode();
      result = 31 * result + fieldName.hashCode();
      return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      final FunctionValues targetFuncValues = targetValueSource.getValues(context, readerContext);
      // The key is a pair of leaf reader with a docId relative to that reader. The value is a Map from field to Shape.
      final SolrCache<PerSegCacheKey,Shape> cache =
          SolrRequestInfo.getRequestInfo().getReq().getSearcher().getCache(CACHE_KEY_PREFIX + fieldName);
      if (cache == null) {
        return targetFuncValues; // no caching; no configured cache
      }

      return new FunctionValues() {
        int docId = -1;
        Shape shape = null;

        private void setShapeFromDoc(int doc) {
          if (docId == doc) {
            return;
          }
          docId = doc;
          //lookup in cache
          PerSegCacheKey key = new PerSegCacheKey(readerContext.reader().getCoreCacheKey(), doc);
          shape = cache.get(key);
          if (shape == null) {
            shape = (Shape) targetFuncValues.objectVal(doc);
            if (shape != null) {
              cache.put(key, shape);
            }
          } else {
            //optimize shape on a cache hit if possible. This must be thread-safe and it is.
            if (shape instanceof JtsGeometry) {
              ((JtsGeometry) shape).index(); // TODO would be nice if some day we didn't have to cast
            }
          }
        }

        // Use the cache for exists & objectVal;

        @Override
        public boolean exists(int doc) {
          setShapeFromDoc(doc);
          return shape != null;
        }

        @Override
        public Object objectVal(int doc) {
          setShapeFromDoc(doc);
          return shape;
        }

        @Override
        public Explanation explain(int doc) {
          return targetFuncValues.explain(doc);
        }

        @Override
        public String toString(int doc) {
          return targetFuncValues.toString(doc);
        }
      };

    }

  }

  public static final String CACHE_KEY_PREFIX = "perSegSpatialFieldCache_";//then field name

  // Used in a SolrCache for the key
  private static class PerSegCacheKey {
    final WeakReference<Object> segCoreKeyRef;
    final int docId;
    final int hashCode;//cached because we can't necessarily compute after construction

    private PerSegCacheKey(Object segCoreKey, int docId) {
      this.segCoreKeyRef = new WeakReference<>(segCoreKey);
      this.docId = docId;
      this.hashCode = segCoreKey.hashCode() * 31 + docId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PerSegCacheKey that = (PerSegCacheKey) o;

      if (docId != that.docId) return false;

      //compare by referent not reference
      Object segCoreKey = segCoreKeyRef.get();
      if (segCoreKey == null) {
        return false;
      }
      return segCoreKey.equals(that.segCoreKeyRef.get());
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      return "Key{seg=" + segCoreKeyRef.get() + ", docId=" + docId + '}';
    }
  }
}
