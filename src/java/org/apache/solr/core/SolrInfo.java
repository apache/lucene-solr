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
 * @version $Id$
 */

// MBean pattern for holding various ui friendly strings and URLs
// for use by objects which are 'plugable' to make administering
// production use easier
  // name        - simple common usage name, e.g. BasicQueryHandler
  // version     - simple common usage version, e.g. 2.0
  // description - simple one or two line description
  // cvsId       - yes, really the CVS Id      (type 'man co')
  // cvsName     - yes, really the CVS Name    (type 'man co')
  // cvsSource   - yes, really the CVS Source  (type 'man co')
  // docs        - URL list: TWIKI, Faq, Design doc, something! :)

abstract class SolrInfo implements SolrInfoMBean {
  public static String _cvsId="$Id$";
  public static String _cvsSource="$Source: /cvs/main/searching/solr/solarcore/src/solr/SolrInfo.java,v $";
  public static String _cvsName="$Name:  $";


  public String getName()          { return this.name;        }
  public String getVersion()       { return this.version;     }
  public String getDescription()   { return this.description; }
  public Category getCategory()    { return SolrInfoMBean.Category.QUERYHANDLER; }
  public String getSourceId()         { return this.cvsId;       }
  public String getCvsName()       { return this.cvsName;     }
  public String getSource()     { return this.cvsSource;   }
  public URL[] getDocs()           { return this.docs;        }
  public NamedList getStatistics() { return null;        }


  public void setName(String name )          { this.name        = name;      }
  public void setVersion(String vers)        { this.version     = vers;      }
  public void setDescription(String desc)    { this.description = desc;      }
  public void setCvsId(String cvsId)         { this.cvsId       = cvsId;     }
  public void setCvsName(String cvsName)     { this.cvsName     = cvsName;   }
  public void setCvsSource(String cvsSource) { this.cvsSource   = cvsSource; }
  public void setDocs(URL[] docs)            { this.docs        = docs;      }

  public void addDoc(URL doc)
  {
    if (doc == null) {
      // should throw runtime exception
      return;
    }
    if (docs != null) {
      URL[] newDocs = new URL[docs.length+1];
      int i;
      for (i = 0; i < docs.length; i++) {
        newDocs[i] = docs[i];
      }
      newDocs[i] = doc;
      docs = newDocs;
    } else {
      docs = new URL[1];
      docs[0] = doc;
    }
  }

  public void addDoc(String doc)
  {
    if (doc == null) {
      // should throw runtime exception
      return;
    }
    try {
      URL docURL = new URL(doc);
      addDoc(docURL);
    } catch (Exception e) {
      // ignore for now
    }
  }

  private String name;
  private String version;
  private String description;
  public String cvsId;
  public String cvsSource;
  public String cvsName;
  private URL[] docs;
}
