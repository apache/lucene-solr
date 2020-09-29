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

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.Utils;

/**
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
class ReplicaCount  implements MapWriter {
  long nrt, tlog, pull;

  public ReplicaCount() {
    nrt = tlog = pull = 0;
  }

  public ReplicaCount(long nrt, long tlog, long pull) {
    this.nrt = nrt;
    this.tlog = tlog;
    this.pull = pull;
  }

  public long total() {
    return nrt + tlog + pull;
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if (nrt > 0) ew.put(Replica.Type.NRT.name(), nrt);
    if (pull > 0) ew.put(Replica.Type.PULL.name(), pull);
    if (tlog > 0) ew.put(Replica.Type.TLOG.name(), tlog);
    ew.put("count", total());
  }

  public Long getVal(Replica.Type type) {
    if (type == null) return total();
    switch (type) {
      case NRT:
        return nrt;
      case PULL:
        return pull;
      case TLOG:
        return tlog;
    }
    return total();
  }

  public void increment(List<ReplicaInfo> infos) {
    if (infos == null) return;
    for (ReplicaInfo info : infos) {
      increment(info);
    }
  }

  void increment(ReplicaInfo info) {
    increment(info.getType());
  }

  void increment(ReplicaCount count) {
    nrt += count.nrt;
    pull += count.pull;
    tlog += count.tlog;
  }


  public void increment(Replica.Type type) {
    switch (type) {
      case NRT:
        nrt++;
        break;
      case PULL:
        pull++;
        break;
      case TLOG:
        tlog++;
        break;
      default:
        nrt++;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ReplicaCount) {
      ReplicaCount that = (ReplicaCount) obj;
      return that.nrt == this.nrt && that.tlog == this.tlog && that.pull == this.pull;

    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nrt, tlog, pull);
  }

  @Override
  public String toString() {
    return Utils.toJSONString(this);
  }

  public ReplicaCount copy() {
    return new ReplicaCount(nrt, tlog, pull);
  }

  public void reset() {
    nrt = tlog = pull = 0;
  }

  public int delta(int expectedReplicaCount, Replica.Type type) {
    if (type == Replica.Type.NRT) return (int) (nrt - expectedReplicaCount);
    if (type == Replica.Type.PULL) return (int) (pull - expectedReplicaCount);
    if (type == Replica.Type.TLOG) return (int) (tlog - expectedReplicaCount);
    throw new RuntimeException("NO type");
  }
}
