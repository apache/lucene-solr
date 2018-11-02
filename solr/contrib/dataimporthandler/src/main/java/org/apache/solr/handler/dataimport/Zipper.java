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
package org.apache.solr.handler.dataimport;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.handler.dataimport.DIHCacheSupport.Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

class Zipper {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final DIHCacheSupport.Relation relation;
  
  @SuppressWarnings("rawtypes")
  private Comparable parentId;
  @SuppressWarnings("rawtypes")
  private Comparable lastChildId;
  
  private Iterator<Map<String,Object>> rowIterator;
  private PeekingIterator<Map<String,Object>> peeker;
  
  /** @return initialized zipper or null */
  public static Zipper createOrNull(Context context){
    if("zipper".equals(context.getEntityAttribute("join"))){
      DIHCacheSupport.Relation r = new DIHCacheSupport.Relation(context);
      if(r.doKeyLookup){
        return new Zipper(r); 
      }
    } 
    return null;
  }
  
  
  private Zipper(Relation relation) {
    this.relation = relation;
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  public Map<String,Object> supplyNextChild(
      Iterator<Map<String,Object>> rowIterator) {
    preparePeeker(rowIterator);
      
    while(peeker.hasNext()){
      Map<String,Object> current = peeker.peek();
      Comparable childId = (Comparable) current.get(relation.primaryKey);
      
      if(lastChildId!=null && lastChildId.compareTo(childId)>0){
        throw new IllegalArgumentException("expect increasing foreign keys for "+relation+
            " got: "+lastChildId+","+childId);
      }
      lastChildId = childId;
      int cmp = childId.compareTo(parentId);
      if(cmp==0){
        Map<String,Object> child = peeker.next();
        assert child==current: "peeker should be right but "+current+" != " + child;
        log.trace("yeild child {} entry {}",relation, current);
        return child;// TODO it's for one->many for many->one it should be just peek() 
      }else{
        if(cmp<0){ // child belongs to 10th and parent is 20th, skip for the next one
          Map<String,Object> child = peeker.next();
          assert child==current: "peeker should be right but "+current+" != " + child;
          log.trace("skip child {}, {} > {}",relation, parentId, childId);
        }else{ // child belongs to 20th and  parent is 10th, no more children, go to next parent
          log.trace("childen is over {}, {} < {}", relation, parentId, current);
          return null;
        }
      }
    }
    
    return null;
  }

  private void preparePeeker(Iterator<Map<String,Object>> rowIterator) {
    if(this.rowIterator==null){
      this.rowIterator = rowIterator;
      peeker = Iterators.peekingIterator(rowIterator);
    }else{
      assert this.rowIterator==rowIterator: "rowIterator should never change but "+this.rowIterator+
          " supplied before has been changed to "+rowIterator; 
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void onNewParent(Context context) {
    Comparable newParent = (Comparable) context.resolve(relation.foreignKey);
    if(parentId!=null && parentId.compareTo(newParent)>=0){
      throw new IllegalArgumentException("expect strictly increasing primary keys for "+relation+
          " got: "+parentId+","+newParent);
    }
    log.trace("{}: {}->{}",relation, newParent, parentId);
    parentId = newParent;
  }
  
}
