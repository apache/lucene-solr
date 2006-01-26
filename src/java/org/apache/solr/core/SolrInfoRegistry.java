/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import org.apache.solr.core.SolrInfoMBean;

import java.util.*;

/**
 * @author ronp
 * @version $Id: SolrInfoRegistry.java,v 1.5 2005/05/14 03:34:39 yonik Exp $
 */

// A Registry to hold a collection of SolrInfo objects

public class SolrInfoRegistry {
  public static final String cvsId="$Id: SolrInfoRegistry.java,v 1.5 2005/05/14 03:34:39 yonik Exp $";
  public static final String cvsSource="$Source: /cvs/main/searching/solr/solarcore/src/solr/SolrInfoRegistry.java,v $";
  public static final String cvsName="$Name:  $";

  private static final Map<String,SolrInfoMBean> inst = Collections.synchronizedMap(new LinkedHashMap<String,SolrInfoMBean>());

  public static Map<String, SolrInfoMBean> getRegistry()
  {
    return inst;
  }

}
