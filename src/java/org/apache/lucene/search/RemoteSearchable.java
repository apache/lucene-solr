package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import java.io.IOException;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.RMISecurityManager;
import java.rmi.server.UnicastRemoteObject;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/** A remote searchable implementation. */
public class RemoteSearchable
  extends UnicastRemoteObject
  implements Searchable {
  
  private Searchable local;
  
  /** Constructs and exports a remote searcher. */
  public RemoteSearchable(Searchable local) throws RemoteException {
    super();
    this.local = local;
  }
  
  public void search(Query query, Filter filter, HitCollector results)
    throws IOException {
    local.search(query, filter, results);
  }

  public void close() throws IOException {
    local.close();
  }

  public int docFreq(Term term) throws IOException {
    return local.docFreq(term);
  }

  public int maxDoc() throws IOException {
    return local.maxDoc();
  }

  public TopDocs search(Query query, Filter filter, int n) throws IOException {
    return local.search(query, filter, n);
  }

  public TopFieldDocs search (Query query, Filter filter, int n, Sort sort)
    throws IOException {
    return local.search (query, filter, n, sort);
  }

  public Document doc(int i) throws IOException {
    return local.doc(i);
  }

  public Query rewrite(Query original) throws IOException {
    return local.rewrite(original);
  }

  public Explanation explain(Query query, int doc) throws IOException {
    return local.explain(query, doc);
  }

  /** Exports a searcher for the index in args[0] named
   * "//localhost/Searchable". */
  public static void main(String args[]) throws Exception {
    // create and install a security manager
    if (System.getSecurityManager() == null) {
      System.setSecurityManager(new RMISecurityManager());
    }
    
    Searchable local = new IndexSearcher(args[0]);
    RemoteSearchable impl = new RemoteSearchable(local);
      
    // bind the implementation to "Searchable"
    Naming.rebind("//localhost/Searchable", impl);
  }

}
