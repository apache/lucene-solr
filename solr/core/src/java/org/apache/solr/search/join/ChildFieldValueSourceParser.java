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
package org.apache.solr.search.join;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ToParentBlockJoinSortField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.search.join.BlockJoinParentQParser.AllParentsAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChildFieldValueSourceParser extends ValueSourceParser {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final class BlockJoinSortFieldValueSource extends ValueSource {
    private static final class BytesToStringComparator extends FieldComparator<String> {
      private final FieldComparator<BytesRef> byteRefs;

      private BytesToStringComparator(FieldComparator<BytesRef> byteRefs) {
        this.byteRefs = byteRefs;
      }

      @Override
      public int compare(int slot1, int slot2) {
        return byteRefs.compare(slot1, slot2);
      }

      @Override
      public void setTopValue(String value) {
        byteRefs.setTopValue(new BytesRef(value));
      }

      @Override
      public String value(int slot) {
        final BytesRef value = byteRefs.value(slot);
        return value!=null ? value.utf8ToString() : null;
      }

      @Override
      public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return byteRefs.getLeafComparator(context);
      }
    }

    private final BitSetProducer childFilter;
    private final BitSetProducer parentFilter;
    private final SchemaField childField;

    private BlockJoinSortFieldValueSource(BitSetProducer childFilter, BitSetProducer parentFilter,
        SchemaField childField) {
      this.childFilter = childFilter;
      this.parentFilter = parentFilter;
      this.childField = childField;
    }


    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((childField == null) ? 0 : childField.hashCode());
      result = prime * result + ((childFilter == null) ? 0 : childFilter.hashCode());
      result = prime * result + ((parentFilter == null) ? 0 : parentFilter.hashCode());
      return result;
    }


    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      BlockJoinSortFieldValueSource other = (BlockJoinSortFieldValueSource) obj;
      if (childField == null) {
        if (other.childField != null) return false;
      } else if (!childField.equals(other.childField)) return false;
      if (childFilter == null) {
        if (other.childFilter != null) return false;
      } else if (!childFilter.equals(other.childFilter)) return false;
      if (parentFilter == null) {
        if (other.parentFilter != null) return false;
      } else if (!parentFilter.equals(other.parentFilter)) return false;
      return true;
    }
    

    @Override
    public String toString() {
      return "BlockJoinSortFieldValueSource [childFilter=" + childFilter + ", parentFilter=" + parentFilter
          + ", childField=" + childField + "]";
    }

    @Override
    public SortField getSortField(boolean reverse) {
      final Type type = childField.getSortField(reverse).getType();
        return new ToParentBlockJoinSortField(childField.getName(), 
            type, reverse, 
            parentFilter, childFilter) {
          @Override
          public FieldComparator<?> getComparator(int numHits, int sortPos) {
            final FieldComparator<?> comparator = super.getComparator(numHits, sortPos);
            return type ==Type.STRING ?  new BytesToStringComparator((FieldComparator<BytesRef>) comparator): comparator;
          }
        };
    }

    @Override
    public String description() {
      return NAME + " for " + childField.getName() +" of "+ query(childFilter);
    }

    private String query(BitSetProducer bits) {
      return (bits instanceof QueryBitSetProducer) ? ((QueryBitSetProducer) bits).getQuery().toString()
          : bits.toString();
    }

    @Override
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
      throw new UnsupportedOperationException(this + " is only for sorting");
    }
  }

  public static final String NAME = "childfield";

  @Override
  public ValueSource parse(FunctionQParser fp) throws SyntaxError {
    
    final String sortFieldName = fp.parseArg();
    final Query query;
    if (fp.hasMoreArguments()){
      query = fp.parseNestedQuery();
    } else {
      query = fp.subQuery(fp.getParam(CommonParams.Q), null).getQuery();
    }
    
    BitSetProducer parentFilter;
    BitSetProducer childFilter;
    SchemaField sf;
    try {
      AllParentsAware bjQ;
      if (!(query instanceof AllParentsAware)) {
        throw new SyntaxError("expect a reference to block join query "+
              AllParentsAware.class.getSimpleName()+" in "+fp.getString());
      }
      bjQ = (AllParentsAware) query;
      
      parentFilter = BlockJoinParentQParser.getCachedFilter(fp.getReq(), bjQ.getParentQuery()).getFilter();
      childFilter = BlockJoinParentQParser.getCachedFilter(fp.getReq(), bjQ.getChildQuery()).getFilter();

      if (sortFieldName==null || sortFieldName.equals("")) {
        throw new SyntaxError ("field is omitted in "+fp.getString());
      }
      
      sf = fp.getReq().getSchema().getFieldOrNull(sortFieldName);
      if (null == sf) {
        throw new SyntaxError
          (NAME+" sort param field \""+ sortFieldName+"\" can't be found in schema");
      }
    } catch (SyntaxError e) {
      log.error("can't parse "+fp.getString(), e);
      throw e;
    }
    return new BlockJoinSortFieldValueSource(childFilter, parentFilter, sf);
  }
}
