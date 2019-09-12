/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

solrAdminApp.controller('CloudController',
    function($scope, $location, Zookeeper, Constants, Collections, System, Metrics, ZookeeperStatus) {

        $scope.showDebug = false;

        $scope.$on("cloud-dump", function(event) {
            $scope.showDebug = true;
        });

        $scope.closeDebug = function() {
            $scope.showDebug = false;
        };

        var view = $location.search().view ? $location.search().view : "nodes";
        if (view === "tree") {
            $scope.resetMenu("cloud-tree", Constants.IS_ROOT_PAGE);
            treeSubController($scope, Zookeeper);
        } else if (view === "graph") {
            $scope.resetMenu("cloud-graph", Constants.IS_ROOT_PAGE);
            graphSubController($scope, Zookeeper, false);
        } else if (view === "nodes") {
            $scope.resetMenu("cloud-nodes", Constants.IS_ROOT_PAGE);
            nodesSubController($scope, Collections, System, Metrics);
        } else if (view === "zkstatus") {
            $scope.resetMenu("cloud-zkstatus", Constants.IS_ROOT_PAGE);
            zkStatusSubController($scope, ZookeeperStatus, false);
        }
    }
);

function getOrCreateObj(name, object) {
  if (name in object) {
    entry = object[name];
  } else {
    entry = {};
    object[name] = entry;
  }
  return entry;
}

function getOrCreateList(name, object) {
  if (name in object) {
    entry = object[name];
  } else {
    entry = [];
    object[name] = entry;
  }
  return entry;
}

function ensureInList(string, list) {
  if (list.indexOf(string) === -1) {
    list.push(string);
  }
}

/* Puts a node name into the hosts structure */
function ensureNodeInHosts(node_name, hosts) {
  var hostName = node_name.split(":")[0];
  var host = getOrCreateObj(hostName, hosts);
  var hostNodes = getOrCreateList("nodes", host);
  ensureInList(node_name, hostNodes);
}

// from http://scratch99.com/web-development/javascript/convert-bytes-to-mb-kb/
function bytesToSize(bytes) {
  var sizes = ['b', 'Kb', 'Mb', 'Gb', 'Tb'];
  if (bytes === 0) return '0b';
  var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
  if (bytes === 0) return bytes + '' + sizes[i];
  return (bytes / Math.pow(1024, i)).toFixed(1) + '' + sizes[i];
}

function numDocsHuman(docs) {
  var sizes = ['', 'k', 'mn', 'bn', 'tn'];
  if (docs === 0) return '0';
  var i = parseInt(Math.floor(Math.log(docs) / Math.log(1000)));
  if (i === 0) return docs + '' + sizes[i];
  return (docs / Math.pow(1000, i)).toFixed(1) + '' + sizes[i];
}

/* Returns a style class depending on percentage */
var styleForPct = function (pct) {
  if (pct < 60) return "pct-normal";
  if (pct < 80) return "pct-warn";
  return "pct-critical"
};

function isNumeric(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function coreNameToLabel(name) {
  return name.replace(/(.*?)_shard((\d+_?)+)_replica_?[ntp]?(\d+)/, '\$1_s\$2r\$4');
}

var nodesSubController = function($scope, Collections, System, Metrics) {
  $scope.pageSize = 10;
  $scope.showNodes = true;
  $scope.showTree = false;
  $scope.showGraph = false;
  $scope.showData = false;
  $scope.showAllDetails = false;
  $scope.showDetails = {};
  $scope.from = 0;
  $scope.to = $scope.pageSize - 1;
  $scope.filterType = "node"; // Pre-initialize dropdown

  $scope.toggleAllDetails = function() {
    $scope.showAllDetails = !$scope.showAllDetails;
    for (var node in $scope.nodes) {
      $scope.showDetails[node] = $scope.showAllDetails;
    }
    for (var host in $scope.hosts) {
      $scope.showDetails[host] = $scope.showAllDetails;
    }
  };

  $scope.toggleDetails = function(key) {
    $scope.showDetails[key] = !$scope.showDetails[key] === true;
  };

  $scope.toggleHostDetails = function(key) {
    $scope.showDetails[key] = !$scope.showDetails[key] === true;
    for (var nodeId in $scope.hosts[key].nodes) {
      var node = $scope.hosts[key].nodes[nodeId];
      $scope.showDetails[node] = $scope.showDetails[key];
    }
  };

  $scope.nextPage = function() {
    $scope.from += parseInt($scope.pageSize);
    $scope.reload();
  };

  $scope.previousPage = function() {
    $scope.from = Math.max(0, $scope.from - parseInt($scope.pageSize));
    $scope.reload();
  };
  
  // Checks if this node is the first (alphabetically) for a given host. Used to decide rowspan in table
  $scope.isFirstNodeForHost = function(node) {
    var hostName = node.split(":")[0]; 
    var nodesInHost = $scope.filteredNodes.filter(function (node) {
      return node.startsWith(hostName);
    });
    return nodesInHost[0] === node;
  };
  
  // Returns the first live node for this host, to make sure we pick host-level metrics from a live node
  $scope.firstLiveNodeForHost = function(key) {
    var hostName = key.split(":")[0]; 
    var liveNodesInHost = $scope.filteredNodes.filter(function (key) {
      return key.startsWith(hostName);
    }).filter(function (key) {
      return $scope.live_nodes.includes(key);
    });
    return liveNodesInHost.length > 0 ? liveNodesInHost[0] : key; 
  };

  // Initializes the cluster state, list of nodes, collections etc
  $scope.initClusterState = function() {
    var nodes = {};
    var hosts = {};
    var live_nodes = [];

    // We build a node-centric view of the cluster state which we can easily consume to render the table
    Collections.status(function (data) {
      // Fetch cluster state from collections API and invert to a nodes structure
      for (var name in data.cluster.collections) {
        var collection = data.cluster.collections[name];
        collection.name = name;
        var shards = collection.shards;
        collection.shards = [];
        for (var shardName in shards) {
          var shard = shards[shardName];
          shard.name = shardName;
          shard.collection = collection.name;
          var replicas = shard.replicas;
          shard.replicas = [];
          for (var replicaName in replicas) {
            var core = replicas[replicaName];
            core.name = replicaName;
            core.label = coreNameToLabel(core['core']);
            core.collection = collection.name;
            core.shard = shard.name;
            core.shard_state = shard.state;

            var node_name = core['node_name'];
            var node = getOrCreateObj(node_name, nodes);
            var cores = getOrCreateList("cores", node);
            cores.push(core);
            node['base_url'] = core.base_url;
            node['id'] = core.base_url.replace(/[^\w\d]/g, '');
            node['host'] = node_name.split(":")[0];
            var collections = getOrCreateList("collections", node);
            ensureInList(core.collection, collections);
            ensureNodeInHosts(node_name, hosts);
          }
        }
      }

      live_nodes = data.cluster.live_nodes;
      for (n in data.cluster.live_nodes) {
        node = data.cluster.live_nodes[n];
        if (!(node in nodes)) {
          var hostName = node.split(":")[0];
          nodes[node] = {};
          nodes[node]['host'] = hostName;
        }
        ensureNodeInHosts(node, hosts);
      }

      // Make sure nodes are sorted alphabetically to align with rowspan in table 
      for (var host in hosts) {
        hosts[host].nodes.sort();
      }

      $scope.nodes = nodes;
      $scope.hosts = hosts;
      $scope.live_nodes = live_nodes;

      $scope.Math = window.Math;
      $scope.reload();
    });
  };

  $scope.filterInput = function() {
    $scope.from = 0;
    $scope.to = $scope.pageSize - 1;
    $scope.reload();
  };

  /*
    Reload will fetch data for the current page of the table and thus refresh numbers.
    It is also called whenever a filter or paging action is executed 
   */
  $scope.reload = function() {
    var nodes = $scope.nodes;
    var node_keys = Object.keys(nodes);
    var hosts = $scope.hosts;
    var live_nodes = $scope.live_nodes;
    var hostNames = Object.keys(hosts);
    hostNames.sort();
    var pageSize = isNumeric($scope.pageSize) ? $scope.pageSize : 10;

    // Calculate what nodes that will show on this page
    var nodesToShow = [];
    var nodesParam;
    var hostsToShow = [];
    var filteredNodes;
    var filteredHosts;
    var isFiltered = false;
    switch ($scope.filterType) {
      case "node":  // Find what nodes match the node filter
        if ($scope.nodeFilter) {
          filteredNodes = node_keys.filter(function (node) {
            return node.indexOf($scope.nodeFilter) !== -1;
          });
        }
        break;

      case "collection": // Find what collections match the collection filter and what nodes that have these collections
        if ($scope.collectionFilter) {
          candidateNodes = {};
          nodesCollections = [];
          for (var i = 0 ; i < node_keys.length ; i++) {
            var node_name = node_keys[i];
            var node = nodes[node_name];
            nodeColl = {};
            nodeColl['node'] = node_name;
            collections = {};
            node.cores.forEach(function(core) {
              collections[core.collection] = true;
            });
            nodeColl['collections'] = Object.keys(collections);
            nodesCollections.push(nodeColl);
          }
          nodesCollections.forEach(function(nc) {
            matchingColls = nc['collections'].filter(function (collection) {
              return collection.indexOf($scope.collectionFilter) !== -1;
            });
            if (matchingColls.length > 0) {
              candidateNodes[nc.node] = true;
            }
          });
          filteredNodes = Object.keys(candidateNodes);
        }
        break;

      case "health":

    }
    
    if (filteredNodes) {
      // If filtering is active, calculate what hosts contain the nodes that match the filters
      isFiltered = true;
      filteredHosts = filteredNodes.map(function (node) {
        return node.split(":")[0];
      }).filter(function (item, index, self) {
        return self.indexOf(item) === index;
      });
    } else {
      filteredNodes = node_keys;
      filteredHosts = hostNames;
    }
    filteredNodes.sort();
    filteredHosts.sort();
    
    // Find what hosts & nodes (from the filtered set) that should be displayed on current page
    for (var id = $scope.from ; id < $scope.from + pageSize && filteredHosts[id] ; id++) {
      var hostName = filteredHosts[id];
      hostsToShow.push(hostName);
      if (isFiltered) { // Only show the nodes per host matching active filter
        nodesToShow = nodesToShow.concat(filteredNodes.filter(function (node) {
          return node.startsWith(hostName);
        }));
      } else {
        nodesToShow = nodesToShow.concat(hosts[hostName]['nodes']);
      }
    }
    nodesParam = nodesToShow.filter(function (node) {
      return live_nodes.includes(node); 
    }).join(',');
    var deadNodes = nodesToShow.filter(function (node) {
      return !live_nodes.includes(node);
    });
    deadNodes.forEach(function (node) {
      nodes[node]['dead'] = true;
    });
    $scope.nextEnabled = $scope.from + pageSize < filteredHosts.length;
    $scope.prevEnabled = $scope.from - pageSize >= 0;
    nodesToShow.sort();
    hostsToShow.sort();

    /*
     Fetch system info for all selected nodes
     Pick the data we want to display and add it to the node-centric data structure
      */
    System.get({"nodes": nodesParam}, function (systemResponse) {
      for (var node in systemResponse) {
        if (node in nodes) {
          var s = systemResponse[node];
          nodes[node]['system'] = s;
          var memTotal = s.system.totalPhysicalMemorySize;
          var memFree = s.system.freePhysicalMemorySize;
          var memPercentage = Math.floor((memTotal - memFree) / memTotal * 100);
          nodes[node]['memUsedPct'] = memPercentage;
          nodes[node]['memUsedPctStyle'] = styleForPct(memPercentage);
          nodes[node]['memTotal'] = bytesToSize(memTotal);
          nodes[node]['memFree'] = bytesToSize(memFree);
          nodes[node]['memUsed'] = bytesToSize(memTotal - memFree);

          var heapTotal = s.jvm.memory.raw.total;
          var heapFree = s.jvm.memory.raw.free;
          var heapPercentage = Math.floor((heapTotal - heapFree) / heapTotal * 100);
          nodes[node]['heapUsed'] = bytesToSize(heapTotal - heapFree);
          nodes[node]['heapUsedPct'] = heapPercentage;
          nodes[node]['heapUsedPctStyle'] = styleForPct(heapPercentage);
          nodes[node]['heapTotal'] = bytesToSize(heapTotal);
          nodes[node]['heapFree'] = bytesToSize(heapFree);

          var jvmUptime = s.jvm.jmx.upTimeMS / 1000; // Seconds
          nodes[node]['jvmUptime'] = secondsForHumans(jvmUptime);
          nodes[node]['jvmUptimeSec'] = jvmUptime;

          nodes[node]['uptime'] = s.system.uptime.replace(/.*up (.*?,.*?),.*/, "$1");
          nodes[node]['loadAvg'] = Math.round(s.system.systemLoadAverage * 100) / 100;
          nodes[node]['cpuPct'] = Math.ceil(s.system.processCpuLoad);
          nodes[node]['cpuPctStyle'] = styleForPct(Math.ceil(s.system.processCpuLoad));
          nodes[node]['maxFileDescriptorCount'] = s.system.maxFileDescriptorCount;
          nodes[node]['openFileDescriptorCount'] = s.system.openFileDescriptorCount;
        }
      }
    });

    /*
     Fetch metrics for all selected nodes. Only pull the metrics that we'll show to save bandwidth
     Pick the data we want to display and add it to the node-centric data structure
      */
    Metrics.get({
          "nodes": nodesParam,
          "prefix": "CONTAINER.fs,org.eclipse.jetty.server.handler.DefaultHandler.get-requests,INDEX.sizeInBytes,SEARCHER.searcher.numDocs,SEARCHER.searcher.deletedDocs,SEARCHER.searcher.warmupTime"
        },
        function (metricsResponse) {
          for (var node in metricsResponse) {
            if (node in nodes) {
              var m = metricsResponse[node];
              nodes[node]['metrics'] = m;
              var diskTotal = m.metrics['solr.node']['CONTAINER.fs.totalSpace'];
              var diskFree = m.metrics['solr.node']['CONTAINER.fs.usableSpace'];
              var diskPercentage = Math.floor((diskTotal - diskFree) / diskTotal * 100);
              nodes[node]['diskUsedPct'] = diskPercentage;
              nodes[node]['diskUsedPctStyle'] = styleForPct(diskPercentage);
              nodes[node]['diskTotal'] = bytesToSize(diskTotal);
              nodes[node]['diskFree'] = bytesToSize(diskFree);

              var r = m.metrics['solr.jetty']['org.eclipse.jetty.server.handler.DefaultHandler.get-requests'];
              nodes[node]['req'] = r.count;
              nodes[node]['req1minRate'] = Math.floor(r['1minRate'] * 100) / 100;
              nodes[node]['req5minRate'] = Math.floor(r['5minRate'] * 100) / 100;
              nodes[node]['req15minRate'] = Math.floor(r['15minRate'] * 100) / 100;
              nodes[node]['reqp75_ms'] = Math.floor(r['p75_ms']);
              nodes[node]['reqp95_ms'] = Math.floor(r['p95_ms']);
              nodes[node]['reqp99_ms'] = Math.floor(r['p99_ms']);

              var cores = nodes[node]['cores'];
              var indexSizeTotal = 0;
              var docsTotal = 0;
              var graphData = [];
              if (cores) {
                for (coreId in cores) {
                  var core = cores[coreId];
                  var keyName = "solr.core." + core['core'].replace(/(.*?)_(shard(\d+_?)+)_(replica.*?)/, '\$1.\$2.\$4');
                  var nodeMetric = m.metrics[keyName];
                  var size = nodeMetric['INDEX.sizeInBytes'];
                  size = (typeof size !== 'undefined') ? size : 0;
                  core['sizeInBytes'] = size;
                  core['size'] = bytesToSize(size);
                  if (core['shard_state'] !== 'active' || core['state'] !== 'active') {
                    // If core state is not active, display the real state, or if shard is inactive, display that
                    var labelState = (core['state'] !== 'active') ? core['state'] : core['shard_state'];
                    core['label'] += "_(" + labelState + ")";
                  }
                  indexSizeTotal += size;
                  var numDocs = nodeMetric['SEARCHER.searcher.numDocs'];
                  numDocs = (typeof numDocs !== 'undefined') ? numDocs : 0;
                  core['numDocs'] = numDocs;
                  core['numDocsHuman'] = numDocsHuman(numDocs);
                  core['avgSizePerDoc'] = bytesToSize(numDocs === 0 ? 0 : size / numDocs);
                  var deletedDocs = nodeMetric['SEARCHER.searcher.deletedDocs'];
                  deletedDocs = (typeof deletedDocs !== 'undefined') ? deletedDocs : 0;
                  core['deletedDocs'] = deletedDocs;
                  core['deletedDocsHuman'] = numDocsHuman(deletedDocs);
                  var warmupTime = nodeMetric['SEARCHER.searcher.warmupTime'];
                  warmupTime = (typeof warmupTime !== 'undefined') ? warmupTime : 0;
                  core['warmupTime'] = warmupTime;
                  docsTotal += core['numDocs'];
                }
                for (coreId in cores) {
                  core = cores[coreId];
                  var graphObj = {};
                  graphObj['label'] = core['label'];
                  graphObj['size'] = core['sizeInBytes'];
                  graphObj['sizeHuman'] = core['size'];
                  graphObj['pct'] = (core['sizeInBytes'] / indexSizeTotal) * 100;
                  graphData.push(graphObj);
                }
                cores.sort(function (a, b) {
                  return b.sizeInBytes - a.sizeInBytes
                });
              } else {
                cores = {};
              }
              graphData.sort(function (a, b) {
                return b.size - a.size
              });
              nodes[node]['graphData'] = graphData;
              nodes[node]['numDocs'] = numDocsHuman(docsTotal);
              nodes[node]['sizeInBytes'] = indexSizeTotal;
              nodes[node]['size'] = bytesToSize(indexSizeTotal);
              nodes[node]['sizePerDoc'] = docsTotal === 0 ? '0b' : bytesToSize(indexSizeTotal / docsTotal);

              // Build the d3 powered bar chart
              $('#chart' + nodes[node]['id']).empty();
              var chart = d3.select('#chart' + nodes[node]['id']).append('div').attr('class', 'chart');

              // Add one div per bar which will group together both labels and bars
              var g = chart.selectAll('div')
                  .data(nodes[node]['graphData']).enter()
                  .append('div');

              // Add the bars
              var bars = g.append("div")
                  .attr("class", "rect")
                  .text(function (d) {
                    return d.label + ':\u00A0\u00A0' + d.sizeHuman;
                  });

              // Execute the transition to show the bars
              bars.transition()
                  .ease('elastic')
                  .style('width', function (d) {
                    return d.pct + '%';
                  });
            }
          }
        });
    $scope.nodes = nodes;
    $scope.hosts = hosts;
    $scope.live_nodes = live_nodes;
    $scope.nodesToShow = nodesToShow;
    $scope.hostsToShow = hostsToShow;
    $scope.filteredNodes = filteredNodes;
    $scope.filteredHosts = filteredHosts;
  };
  $scope.initClusterState();
};

var zkStatusSubController = function($scope, ZookeeperStatus) {
    $scope.showZkStatus = true;
    $scope.showNodes = false;
    $scope.showTree = false;
    $scope.showGraph = false;
    $scope.tree = {};
    $scope.showData = false;
    $scope.showDetails = false;
    
    $scope.toggleDetails = function() {
      $scope.showDetails = !$scope.showDetails === true;
    };

    $scope.initZookeeper = function() {
      ZookeeperStatus.monitor({}, function(data) {
        $scope.zkState = data.zkStatus;
        $scope.mainKeys = ["ok", "clientPort", "secureClientPort", "zk_server_state", "zk_version",
          "zk_approximate_data_size", "zk_znode_count", "zk_num_alive_connections"];
        $scope.detailKeys = ["dataDir", "dataLogDir", 
          "zk_avg_latency", "zk_max_file_descriptor_count", "zk_watch_count", 
          "zk_packets_sent", "zk_packets_received",
          "tickTime", "maxClientCnxns", "minSessionTimeout", "maxSessionTimeout"];
        $scope.ensembleMainKeys = ["serverId", "electionPort", "quorumPort"];
        $scope.ensembleDetailKeys = ["peerType", "electionAlg", "initLimit", "syncLimit",
          "zk_followers", "zk_synced_followers", "zk_pending_syncs",
          "server.1", "server.2", "server.3", "server.4", "server.5"];
        $scope.notEmptyRow = function(key) {
          for (hostId in $scope.zkState.details) {
            if (key in $scope.zkState.details[hostId]) return true;
          }
          return false;
        };
      });
    };

    $scope.initZookeeper();
};

var treeSubController = function($scope, Zookeeper) {
    $scope.showZkStatus = false;
    $scope.showTree = true;
    $scope.showGraph = false;
    $scope.tree = {};
    $scope.showData = false;

    $scope.showTreeLink = function(link) {
        var path = decodeURIComponent(link.replace(/.*[\\?&]path=([^&#]*).*/, "$1"));
        Zookeeper.detail({path: path}, function(data) {
            $scope.znode = data.znode;
            var path = data.znode.path.split( '.' );
            if(path.length >1) {
              $scope.lang = path.pop();
            } else {
              var lastPathElement = data.znode.path.split( '/' ).pop();
              if (lastPathElement == "managed-schema") {
                  $scope.lang = "xml";
              }
            }
            $scope.showData = true;
        });
    };

    $scope.hideData = function() {
        $scope.showData = false;
    };

    $scope.initTree = function() {
      Zookeeper.simple(function(data) {
        $scope.tree = data.tree;
      });
    };

    $scope.initTree();
};

/**
 * Translates seconds into human readable format of seconds, minutes, hours, days, and years
 * 
 * @param  {number} seconds The number of seconds to be processed
 * @return {string}         The phrase describing the the amount of time
 */
function secondsForHumans ( seconds ) {
    var levels = [
        [Math.floor(seconds / 31536000), 'y'],
        [Math.floor((seconds % 31536000) / 86400), 'd'],
        [Math.floor(((seconds % 31536000) % 86400) / 3600), 'h'],
        [Math.floor((((seconds % 31536000) % 86400) % 3600) / 60), 'm']
    ];
    var returntext = '';

    for (var i = 0, max = levels.length; i < max; i++) {
        if ( levels[i][0] === 0 ) continue;
        returntext += ' ' + levels[i][0] + levels[i][1];
    }
    return returntext.trim() === '' ? '0m' : returntext.trim();
}

var graphSubController = function ($scope, Zookeeper) {
    $scope.showZkStatus = false;
    $scope.showTree = false;
    $scope.showGraph = true;

    $scope.filterType = "status";

    $scope.helperData = {
        protocol: [],
        host: [],
        hostname: [],
        port: [],
        pathname: [],
        replicaType: [],
        base_url: [],
        core: [],
        node_name: [],
        state: [],
        core_node: []
    };

    $scope.next = function() {
        $scope.pos += $scope.rows;
        $scope.initGraph();
    };

    $scope.previous = function() {
        $scope.pos = Math.max(0, $scope.pos - $scope.rows);
        $scope.initGraph();
    };

    $scope.resetGraph = function() {
        $scope.pos = 0;
        $scope.initGraph();
    };

    $scope.initGraph = function() {
        Zookeeper.liveNodes(function (data) {
            var live_nodes = {};
            for (var c in data.tree[0].children) {
                live_nodes[data.tree[0].children[c].data.title] = true;
            }

            var params = {view: "graph"};
            if ($scope.rows) {
                params.start = $scope.pos;
                params.rows = $scope.rows;
            }

            var filter = ($scope.filterType=='status') ? $scope.pagingStatusFilter : $scope.pagingFilter;

            if (filter) {
                params.filterType = $scope.filterType;
                params.filter = filter;
            }

            Zookeeper.clusterState(params, function (data) {
                    eval("var state=" + data.znode.data); // @todo fix horrid means to parse JSON

                    var leaf_count = 0;
                    var graph_data = {
                        name: null,
                        children: []
                    };

                    for (var c in state) {
                        var shards = [];
                        for (var s in state[c].shards) {
                            var shard_status = state[c].shards[s].state;
                            shard_status = shard_status == 'inactive' ? 'shard-inactive' : shard_status;
                            var nodes = [];
                            for (var n in state[c].shards[s].replicas) {
                                leaf_count++;
                                var replica = state[c].shards[s].replicas[n]

                                var uri = replica.base_url;
                                var parts = uri.match(/^(\w+:)\/\/(([\w\d\.-]+)(:(\d+))?)(.+)$/);
                                var uri_parts = {
                                    protocol: parts[1],
                                    host: parts[2],
                                    hostname: parts[3],
                                    port: parseInt(parts[5] || 80, 10),
                                    pathname: parts[6],
                                    replicaType: replica.type,
                                    base_url: replica.base_url,
                                    core: replica.core,
                                    node_name: replica.node_name,
                                    state: replica.state,
                                    core_node: n
                                };

                                $scope.helperData.protocol.push(uri_parts.protocol);
                                $scope.helperData.host.push(uri_parts.host);
                                $scope.helperData.hostname.push(uri_parts.hostname);
                                $scope.helperData.port.push(uri_parts.port);
                                $scope.helperData.pathname.push(uri_parts.pathname);
                                $scope.helperData.replicaType.push(uri_parts.replicaType);
                                $scope.helperData.base_url.push(uri_parts.base_url);
                                $scope.helperData.core.push(uri_parts.core);
                                $scope.helperData.node_name.push(uri_parts.node_name);
                                $scope.helperData.state.push(uri_parts.state);
                                $scope.helperData.core_node.push(uri_parts.core_node);

                                var replica_status = replica.state;

                                if (!live_nodes[replica.node_name]) {
                                    replica_status = 'gone';
                                } else if(shard_status=='shard-inactive') {
                                    replica_status += ' ' + shard_status;
                                }

                                var node = {
                                    name: uri,
                                    data: {
                                        type: 'node',
                                        state: replica_status,
                                        leader: 'true' === replica.leader,
                                        uri: uri_parts
                                    }
                                };
                                nodes.push(node);
                            }

                            var shard = {
                                name: shard_status == "shard-inactive" ? s + ' (inactive)' : s,
                                data: {
                                    type: 'shard',
                                    state: shard_status,
                                    range: state[c].shards[s].range

                                },
                                children: nodes
                            };
                            shards.push(shard);
                        }

                        var collection = {
                            name: c,
                            data: {
                                type: 'collection',
                                pullReplicas: state[c].pullReplicas,
                                replicationFactor: state[c].replicationFactor,
                                router: state[c].router.name,
                                maxShardsPerNode: state[c].maxShardsPerNode,
                                autoAddReplicas: state[c].autoAddReplicas,
                                nrtReplicas: state[c].nrtReplicas,
                                tlogReplicas: state[c].tlogReplicas,
                                numShards: shards.length
                            },
                            children: shards
                        };
                        graph_data.children.push(collection);
                    }
                    $scope.helperData.protocol = $.unique($scope.helperData.protocol);
                    $scope.helperData.host = $.unique($scope.helperData.host);
                    $scope.helperData.hostname = $.unique($scope.helperData.hostname);
                    $scope.helperData.port = $.unique($scope.helperData.port);
                    $scope.helperData.pathname = $.unique($scope.helperData.pathname);
                    $scope.helperData.replicaType = $.unique($scope.helperData.replicaType);
                    $scope.helperData.base_url = $.unique($scope.helperData.base_url);
                    $scope.helperData.core = $.unique($scope.helperData.core);
                    $scope.helperData.node_name = $.unique($scope.helperData.node_name);
                    $scope.helperData.state = $.unique($scope.helperData.state);
                    $scope.helperData.core_node = $.unique($scope.helperData.core_node);

                    if (data.znode && data.znode.paging) {
                        $scope.showPaging = true;

                        var parr = data.znode.paging.split('|');
                        if (parr.length < 3) {
                            $scope.showPaging = false;
                        } else {
                            $scope.start = Math.max(parseInt(parr[0]), 0);
                            $scope.prevEnabled = ($scope.start > 0);
                            $scope.rows = parseInt(parr[1]);
                            $scope.total = parseInt(parr[2]);

                            if ($scope.rows == -1) {
                                $scope.showPaging = false;
                            } else {
                                var filterType = parr.length > 3 ? parr[3] : '';

                                if (filterType == '' || filterType == 'none') filterType = 'status';

                                +$('#cloudGraphPagingFilterType').val(filterType);

                                var filter = parr.length > 4 ? parr[4] : '';
                                var page = Math.floor($scope.start / $scope.rows) + 1;
                                var pages = Math.ceil($scope.total / $scope.rows);
                                $scope.last = Math.min($scope.start + $scope.rows, $scope.total);
                                $scope.nextEnabled = ($scope.last < $scope.total);
                            }
                        }
                    }
                    else {
                        $scope.showPaging = false;
                    }
                    $scope.graphData = graph_data;
                    $scope.leafCount = leaf_count;
                });
        });
    };

    $scope.initGraph();
    $scope.pos = 0;
};

solrAdminApp.directive('graph', function(Constants) {
    return {
        restrict: 'EA',
        scope: {
            data: "=",
            leafCount: "=",
            helperData: "=",
        },
        link: function (scope, element, attrs) {
            var helper_path_class = function (p) {
                var classes = ['link'];
                classes.push('lvl-' + p.target.depth);

                if (p.target.data && p.target.data.leader) {
                    classes.push('leader');
                }

                if (p.target.data && p.target.data.state) {
                    classes.push(p.target.data.state);
                }

                return classes.join(' ');
            };

            var helper_node_class = function (d) {
                var classes = ['node'];
                classes.push('lvl-' + d.depth);

                if (d.data && d.data.leader) {
                    classes.push('leader');
                }

                if (d.data && d.data.state) {
                    if(!(d.data.type=='shard' && d.data.state=='active')){
                        classes.push(d.data.state);
                    }
                }

                return classes.join(' ');
            };

            var helper_tooltip_text = function (d) {
                if (!d.data) {
                  return tooltip;
                }
                var tooltip;

                if (! d.data.type) {
                  return tooltip;
                }


                if (d.data.type == 'collection') {
                  tooltip = d.name + " {<br/> ";
                  tooltip += "numShards: [" + d.data.numShards + "],<br/>";
                  tooltip += "maxShardsPerNode: [" + d.data.maxShardsPerNode + "],<br/>";
                  tooltip += "router: [" + d.data.router + "],<br/>";
                  tooltip += "autoAddReplicas: [" + d.data.autoAddReplicas + "],<br/>";
                  tooltip += "replicationFactor: [" + d.data.replicationFactor + "],<br/>";
                  tooltip += "nrtReplicas: [" + d.data.nrtReplicas + "],<br/>";
                  tooltip += "pullReplicas: [" + d.data.pullReplicas + "],<br/>";
                  tooltip += "tlogReplicas: [" + d.data.tlogReplicas + "],<br/>";
                  tooltip += "}";
                } else if (d.data.type == 'shard') {
                  tooltip = d.name + " {<br/> ";
                  tooltip += "range: [" + d.data.range + "],<br/>";
                  tooltip += "state: [" + d.data.state + "],<br/>";
                  tooltip += "}";
                } else if (d.data.type == 'node') {
                  tooltip = d.data.uri.core_node + " {<br/>";

                  if (0 !== scope.helperData.core.length) {
                      tooltip += "core: [" + d.data.uri.core + "],<br/>";
                  }

                  if (0 !== scope.helperData.node_name.length) {
                      tooltip += "node_name: [" + d.data.uri.node_name + "],<br/>";
                  }
                  tooltip += "}";
                }

                return tooltip;
            };

            var helper_node_text = function (d) {
                if (!d.data || !d.data.uri) {
                    return d.name;
                }

                var name = d.data.uri.hostname;
                if (1 !== scope.helperData.protocol.length) {
                    name = d.data.uri.protocol + '//' + name;
                }

                if (1 !== scope.helperData.port.length) {
                    name += ':' + d.data.uri.port;
                }

                if (1 !== scope.helperData.pathname.length) {
                    name += d.data.uri.pathname;
                }

                if(0 !== scope.helperData.replicaType.length) {
                    name += ' (' + d.data.uri.replicaType[0] + ')';
                }

                return name;
            };

            scope.$watch("data", function(newValue, oldValue) {
                if (newValue) {
                    flatGraph(element, scope.data, scope.leafCount);
                }

                $('text').tooltip({
                    content: function() {
                        return $(this).attr('title');
                    }
                });
            });


            function setNodeNavigationBehavior(node, view){
                node
                .attr('data-href', function (d) {
                    if (d.type == "node"){
                        return getNodeUrl(d, view);
                    }
                    else{
                        return "";
                    }
                })
                .on('click', function(d) {
                    if (d.data.type == "node"){
                        location.href = getNodeUrl(d, view);
                    }
                });
            }

            function getNodeUrl(d, view){
                var url = d.name + Constants.ROOT_URL + "#/~cloud";
                if (view != undefined){
                    url += "?view=" + view;
                }
                return url;
            }

            var flatGraph = function(element, graphData, leafCount) {
                var w = element.width(),
                    h = leafCount * 20;

                var tree = d3.layout.tree().size([h, w - 400]);

                var diagonal = d3.svg.diagonal().projection(function (d) {
                    return [d.y, d.x];
                });

                d3.select('#canvas', element).html('');
                var vis = d3.select('#canvas', element).append('svg')
                    .attr('width', w)
                    .attr('height', h)
                    .append('g')
                    .attr('transform', 'translate(100, 0)');

                var nodes = tree.nodes(graphData);

                var link = vis.selectAll('path.link')
                    .data(tree.links(nodes))
                    .enter().append('path')
                    .attr('class', helper_path_class)
                    .attr('d', diagonal);

                var node = vis.selectAll('g.node')
                    .data(nodes)
                    .enter().append('g')
                    .attr('class', helper_node_class)
                    .attr('transform', function (d) {
                        return 'translate(' + d.y + ',' + d.x + ')';
                    })

                node.append('circle')
                    .attr('r', 4.5);

                node.append('text')
                    .attr('dx', function (d) {
                        return 0 === d.depth ? -8 : 8;
                    })
                    .attr('dy', function (d) {
                        return 5;
                    })
                    .attr('text-anchor', function (d) {
                        return 0 === d.depth ? 'end' : 'start';
                    })
                    .attr("title", helper_tooltip_text)
                    .text(helper_node_text);

                setNodeNavigationBehavior(node);
            };
        }
    };
});
