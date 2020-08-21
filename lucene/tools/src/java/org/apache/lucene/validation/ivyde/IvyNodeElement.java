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
package org.apache.lucene.validation.ivyde;

import org.apache.ivy.core.module.id.ModuleRevisionId;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Assists in the further separation of concerns between the view and the Ivy resolve report. The view looks at the
 * IvyNode in a unique way that can lead to expensive operations if we do not achieve this separation.
 * 
 * This class is copied from org/apache/ivyde/eclipse/resolvevisualizer/model/IvyNodeElement.java at 
 * https://svn.apache.org/repos/asf/ant/ivy/ivyde/trunk/org.apache.ivyde.eclipse.resolvevisualizer/src/
 *
 * Changes include: uncommenting generics and converting to diamond operators where appropriate;
 * removing unnecessary casts; removing javadoc tags with no description; and adding a hashCode() implementation.
 */
public class IvyNodeElement {
  private ModuleRevisionId moduleRevisionId;
  private boolean evicted = false;
  private int depth = Integer.MAX_VALUE / 10;
  private Collection<IvyNodeElement> dependencies = new HashSet<>();
  private Collection<IvyNodeElement> callers = new HashSet<>();
  private Collection<IvyNodeElement> conflicts = new HashSet<>();

  /**
   * The caller configurations that caused this node to be reached in the resolution, grouped by caller.
   */
  private Map<IvyNodeElement,String[]>callerConfigurationMap = new HashMap<>();

  /**
   * We try to avoid building the list of this nodes deep dependencies by storing them in this cache by depth level.
   */
  private IvyNodeElement[] deepDependencyCache;
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IvyNodeElement) {
      IvyNodeElement elem = (IvyNodeElement) obj;
      if (elem.getOrganization().equals(getOrganization()) && elem.getName().equals(getName())
          && elem.getRevision().equals(getRevision()))
        return true;
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    int result = 1;
    result = result * 31 + (null == getOrganization() ? 0 : getOrganization().hashCode());
    result = result * 31 + (null == getName() ? 0 : getName().hashCode());
    result = result * 31 + (null == getRevision() ? 0 : getRevision().hashCode());
    return result;
  }

  public IvyNodeElement[] getDependencies() {
    return dependencies.toArray(new IvyNodeElement[dependencies.size()]);
  }

  /**
   * Recursive dependency retrieval
   *
   * @return The array of nodes that represents a node's immediate and transitive dependencies down to an arbitrary
   *         depth.
   */
  public IvyNodeElement[] getDeepDependencies() {
    if (deepDependencyCache == null) {
      Collection<IvyNodeElement> deepDependencies = getDeepDependencies(this);
      deepDependencyCache = deepDependencies.toArray(new IvyNodeElement[deepDependencies.size()]);
    }
    return deepDependencyCache;
  }

  /**
   * Recursive dependency retrieval
   */
  private Collection<IvyNodeElement> getDeepDependencies(IvyNodeElement node) {
    Collection<IvyNodeElement> deepDependencies = new HashSet<>();
    deepDependencies.add(node);

    IvyNodeElement[] directDependencies = node.getDependencies();
    for (int i = 0; i < directDependencies.length; i++) {
      deepDependencies.addAll(getDeepDependencies(directDependencies[i]));
    }

    return deepDependencies;
  }

  /**
   * @return An array of configurations by which this module was resolved
   */
  public String[] getCallerConfigurations(IvyNodeElement caller) {
    return callerConfigurationMap.get(caller);
  }

  public void setCallerConfigurations(IvyNodeElement caller, String[] configurations) {
    callerConfigurationMap.put(caller, configurations);
  }

  public String getOrganization() {
    return moduleRevisionId.getOrganisation();
  }

  public String getName() {
    return moduleRevisionId.getName();
  }

  public String getRevision() {
    return moduleRevisionId.getRevision();
  }

  public boolean isEvicted() {
    return evicted;
  }

  public void setEvicted(boolean evicted) {
    this.evicted = evicted;
  }

  public int getDepth() {
    return depth;
  }

  /**
   * Set this node's depth and recursively update the node's children to relative to the new value.
   */
  public void setDepth(int depth) {
    this.depth = depth;
    for (Iterator<IvyNodeElement> iter = dependencies.iterator(); iter.hasNext();) {
      IvyNodeElement dependency = iter.next();
      dependency.setDepth(depth + 1);
    }
  }

  public IvyNodeElement[] getConflicts() {
    return conflicts.toArray(new IvyNodeElement[conflicts.size()]);
  }

  public void setConflicts(Collection<IvyNodeElement> conflicts) {
    this.conflicts = conflicts;
  }

  public ModuleRevisionId getModuleRevisionId() {
    return moduleRevisionId;
  }

  public void setModuleRevisionId(ModuleRevisionId moduleRevisionId) {
    this.moduleRevisionId = moduleRevisionId;
  }

  public void addCaller(IvyNodeElement caller) {
    callers.add(caller);
    caller.dependencies.add(this);
  }

  public IvyNodeElement[] getCallers() {
    return callers.toArray(new IvyNodeElement[callers.size()]);
  }
}