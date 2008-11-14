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

  protected List<PosCorrectMap> pcmList;
  
  public BaseCharFilter( CharStream in ){
    super(in);
    pcmList = new ArrayList<PosCorrectMap>();
  }

  protected int correctPosition( int currentPos ){
    if( pcmList.isEmpty() ) return currentPos;
    for( int i = pcmList.size() - 1; i >= 0; i-- ){
      if( currentPos >= pcmList.get( i ).pos )
        return currentPos + pcmList.get( i ).cumulativeDiff;
    }
    return currentPos;
  }

  protected static class PosCorrectMap {

    protected int pos;
    protected int cumulativeDiff;

    public PosCorrectMap( int pos, int cumulativeDiff ){
      this.pos = pos;
      this.cumulativeDiff = cumulativeDiff;
    }

    public String toString(){
      StringBuffer sb = new StringBuffer();
      sb.append('(');
      sb.append(pos);
      sb.append(',');
      sb.append(cumulativeDiff);
      sb.append(')');
      return sb.toString();
    }
  }
}
