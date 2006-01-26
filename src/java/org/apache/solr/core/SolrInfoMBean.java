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

import java.net.URL;
import org.apache.solr.util.*;

/**
 * @author ronp
 * @version $Id: SolrInfoMBean.java,v 1.3 2005/05/04 19:15:23 ronp Exp $
 */

// MBean interface for getting various ui friendly strings and URLs
// for use by objects which are 'plugable' to make administering
// production use easier
  // name        - simple common usage name, e.g. BasicQueryHandler
  // version     - simple common usage version, e.g. 2.0
  // description - simple one or two line description
  // cvsId       - yes, really the CVS Id      (type 'man co')
  // cvsName     - yes, really the CVS Name    (type 'man co')
  // cvsSource   - yes, really the CVS Source  (type 'man co')
  // docs        - URL list: TWIKI, Faq, Design doc, something! :)

public interface SolrInfoMBean {

  public enum Category { CORE, QUERYHANDLER, UPDATEHANDLER, CACHE, OTHER };

  public String getName();
  public String getVersion();
  public String getDescription();
  public Category getCategory();
  public String getCvsId();
  public String getCvsName();
  public String getCvsSource();
  public URL[] getDocs();
  public NamedList getStatistics();

}
