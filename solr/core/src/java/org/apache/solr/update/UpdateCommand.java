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
package org.apache.solr.update;

import org.apache.solr.request.SolrQueryRequest;


/** An index update command encapsulated in an object (Command pattern)
 *
 *
 */
public abstract class UpdateCommand implements Cloneable {
  protected SolrQueryRequest req;
  protected long version;
  protected String route;
  protected int flags;

  public static int BUFFERING = 0x00000001;    // update command is being buffered.
  public static int REPLAY    = 0x00000002;    // update command is from replaying a log.
  public static int PEER_SYNC    = 0x00000004; // update command is a missing update being provided by a peer.
  public static int IGNORE_AUTOCOMMIT = 0x00000008; // this update should not count toward triggering of autocommits.
  public static int CLEAR_CACHES = 0x00000010; // clear caches associated with the update log.  used when applying reordered DBQ updates when doing an add.
  public static int IGNORE_INDEXWRITER = 0x00000020;

  public UpdateCommand(SolrQueryRequest req) {
    this.req = req;
  }

  public abstract String name();

  @Override
  public String toString() {
    StringBuilder sb  = new StringBuilder(50);
    sb.append(name());
    sb.append('{');
    boolean needComma = false;
    if (flags != 0) {
      sb.append("flags=").append(Integer.toHexString(flags));
      needComma = true;
    }
    if (version != 0) {
      if (needComma) sb.append(',');
      sb.append("_version_=").append(version);
    }
    return sb.toString();
  }

  public long getVersion() {
    return version;
  }
  public void setVersion(long version) {
    this.version = version;
  }

  public String getRoute() {
    return route;
  }

  public void setRoute (String route) {
    this.route = route;
  }

  public void setFlags(int flags) {
    this.flags = flags;
  }

  public int getFlags() {
    return flags;
  }

  public SolrQueryRequest getReq() {
    return req;
  }

  public void setReq(SolrQueryRequest req) {
    this.req = req;
  }

  @Override
  public UpdateCommand clone() {
    try {
      return (UpdateCommand) super.clone();
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }
}
