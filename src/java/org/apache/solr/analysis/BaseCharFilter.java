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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @version $Id$
 * @since Solr 1.4
 *
 */
public abstract class BaseCharFilter extends CharFilter {

  private List<OffCorrectMap> pcmList;
  
  public BaseCharFilter( CharStream in ){
    super(in);
  }

  protected int correct( int currentOff ){
    if( pcmList == null || pcmList.isEmpty() ) return currentOff;
    for( int i = pcmList.size() - 1; i >= 0; i-- ){
      if( currentOff >= pcmList.get( i ).off )
        return currentOff + pcmList.get( i ).cumulativeDiff;
    }
    return currentOff;
  }
  
  protected int getLastCumulativeDiff(){
    return pcmList == null || pcmList.isEmpty() ? 0 : pcmList.get( pcmList.size() - 1 ).cumulativeDiff;
  }
  
  protected void addOffCorrectMap( int off, int cumulativeDiff ){
    if( pcmList == null ) pcmList = new ArrayList<OffCorrectMap>();
    pcmList.add( new OffCorrectMap( off, cumulativeDiff ) );
  }

  static class OffCorrectMap {

    int off;
    int cumulativeDiff;

    OffCorrectMap( int off, int cumulativeDiff ){
      this.off = off;
      this.cumulativeDiff = cumulativeDiff;
    }

    public String toString(){
      StringBuilder sb = new StringBuilder();
      sb.append('(');
      sb.append(off);
      sb.append(',');
      sb.append(cumulativeDiff);
      sb.append(')');
      return sb.toString();
    }
  }
}
