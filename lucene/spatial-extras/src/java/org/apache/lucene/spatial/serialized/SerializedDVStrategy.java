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
package org.apache.lucene.spatial.serialized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.util.Map;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.io.BinaryCodec;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RandomAccessWeight;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.util.DistanceToShapeValueSource;
import org.apache.lucene.spatial.util.ShapePredicateValueSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;


/**
 * A SpatialStrategy based on serializing a Shape stored into BinaryDocValues.
 * This is not at all fast; it's designed to be used in conjunction with another index based
 * SpatialStrategy that is approximated (like {@link org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy})
 * to add precision or eventually make more specific / advanced calculations on the per-document
 * geometry.
 * The serialization uses Spatial4j's {@link org.locationtech.spatial4j.io.BinaryCodec}.
 *
 * @lucene.experimental
 */
public class SerializedDVStrategy extends SpatialStrategy {

  /**
   * A cache heuristic for the buf size based on the last shape size.
   */
  //TODO do we make this non-volatile since it's merely a heuristic?
  private volatile int indexLastBufSize = 8 * 1024;//8KB default on first run

  /**
   * Constructs the spatial strategy with its mandatory arguments.
   */
  public SerializedDVStrategy(SpatialContext ctx, String fieldName) {
    super(ctx, fieldName);
  }

  @Override
  public Field[] createIndexableFields(Shape shape) {
    int bufSize = Math.max(128, (int) (this.indexLastBufSize * 1.5));//50% headroom over last
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(bufSize);
    final BytesRef bytesRef = new BytesRef();//receiver of byteStream's bytes
    try {
      ctx.getBinaryCodec().writeShape(new DataOutputStream(byteStream), shape);
      //this is a hack to avoid redundant byte array copying by byteStream.toByteArray()
      byteStream.writeTo(new FilterOutputStream(null/*not used*/) {
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          bytesRef.bytes = b;
          bytesRef.offset = off;
          bytesRef.length = len;
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.indexLastBufSize = bytesRef.length;//cache heuristic
    return new Field[]{new BinaryDocValuesField(getFieldName(), bytesRef)};
  }

  @Override
  public ValueSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    //TODO if makeShapeValueSource gets lifted to the top; this could become a generic impl.
    return new DistanceToShapeValueSource(makeShapeValueSource(), queryPoint, multiplier, ctx);
  }

  /**
   * Returns a Query that should be used in a random-access fashion.
   * Use in another manner will be SLOW.
   */
  @Override
  public Query makeQuery(SpatialArgs args) {
    ValueSource shapeValueSource = makeShapeValueSource();
    ShapePredicateValueSource predicateValueSource = new ShapePredicateValueSource(
        shapeValueSource, args.getOperation(), args.getShape());
    return new PredicateValueSourceQuery(predicateValueSource);
  }

  /**
   * Provides access to each shape per document as a ValueSource in which
   * {@link org.apache.lucene.queries.function.FunctionValues#objectVal(int)} returns a {@link
   * Shape}.
   */ //TODO raise to SpatialStrategy
  public ValueSource makeShapeValueSource() {
    return new ShapeDocValueSource(getFieldName(), ctx.getBinaryCodec());
  }

  /** Warning: don't iterate over the results of this query; it's designed for use in a random-access fashion
   * by {@link TwoPhaseIterator}.
   */
  static class PredicateValueSourceQuery extends Query {
    private final ValueSource predicateValueSource;//we call boolVal(doc)

    public PredicateValueSourceQuery(ValueSource predicateValueSource) {
      this.predicateValueSource = predicateValueSource;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      return new RandomAccessWeight(this) {
        @Override
        protected Bits getMatchingDocs(LeafReaderContext context) throws IOException {
          final FunctionValues predFuncValues = predicateValueSource.getValues(null, context);
          return new Bits() {
            @Override
            public boolean get(int index) {
              return predFuncValues.boolVal(index);
            }

            @Override
            public int length() {
              return context.reader().maxDoc();
            }
          };
        }
      };
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             predicateValueSource.equals(((PredicateValueSourceQuery) other).predicateValueSource);
    }

    @Override
    public int hashCode() {
      return classHash() + 31 * predicateValueSource.hashCode();
    }

    @Override
    public String toString(String field) {
      return "PredicateValueSourceQuery(" +
               predicateValueSource.toString() +
             ")";
    }
  }//PredicateValueSourceQuery

  /**
   * Implements a ValueSource by deserializing a Shape in from BinaryDocValues using BinaryCodec.
   * @see #makeShapeValueSource()
   */
  static class ShapeDocValueSource extends ValueSource {

    private final String fieldName;
    private final BinaryCodec binaryCodec;//spatial4j

    private ShapeDocValueSource(String fieldName, BinaryCodec binaryCodec) {
      this.fieldName = fieldName;
      this.binaryCodec = binaryCodec;
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      final BinaryDocValues docValues = readerContext.reader().getBinaryDocValues(fieldName);

      return new FunctionValues() {
        int bytesRefDoc = -1;
        BytesRefBuilder bytesRef = new BytesRefBuilder();

        boolean fillBytes(int doc) {
          if (bytesRefDoc != doc) {
            bytesRef.copyBytes(docValues.get(doc));
            bytesRefDoc = doc;
          }
          return bytesRef.length() != 0;
        }

        @Override
        public boolean exists(int doc) {
          return fillBytes(doc);
        }

        @Override
        public boolean bytesVal(int doc, BytesRefBuilder target) {
          target.clear();
          if (fillBytes(doc)) {
            target.copyBytes(bytesRef);
            return true;
          } else {
            return false;
          }
        }

        @Override
        public Object objectVal(int docId) {
          if (!fillBytes(docId))
            return null;
          DataInputStream dataInput = new DataInputStream(
              new ByteArrayInputStream(bytesRef.bytes(), 0, bytesRef.length()));
          try {
            return binaryCodec.readShape(dataInput);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Explanation explain(int doc) {
          return Explanation.match(Float.NaN, toString(doc));
        }

        @Override
        public String toString(int doc) {
          return description() + "=" + objectVal(doc);//TODO truncate?
        }

      };
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ShapeDocValueSource that = (ShapeDocValueSource) o;

      if (!fieldName.equals(that.fieldName)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = fieldName.hashCode();
      return result;
    }

    @Override
    public String description() {
      return "shapeDocVal(" + fieldName + ")";
    }
  }//ShapeDocValueSource
}
