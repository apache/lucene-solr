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

var solrAdminServices = angular.module('solrAdminServices', ['ngResource']);

solrAdminServices.factory('System',
  ['$resource', function($resource) {
    return $resource('/solr/admin/info/system', {"wt":"json", "_":Date.now()});
  }])
.factory('Cores',
  ['$resource', function($resource) {
    return $resource('/solr/admin/cores',
    {'wt':'json', '_':Date.now()}, {
    "query": {},
    "list": {params:{indexInfo: false}},
    "add": {params:{action: "CREATE"}},
    "unload": {params:{action: "UNLOAD", core: "@core"}},
    "rename": {params:{action: "RENAME"}},
    "swap": {params:{}},
    "reload": {method: "GET", params:{action:"RELOAD", core: "@core"}},
    "optimize": {params:{}}
    });
  }])
.factory('Logging',
  ['$resource', function($resource) {
    return $resource('/solr/admin/info/logging', {'wt':'json', '_':Date.now()}, {
      "events": {params: {since:'0'}},
      "levels": {},
      "setLevel": {}
      });
  }])
.factory('Zookeeper',
  ['$resource', function($resource) {
    return $resource('/solr/zookeeper', {wt:'json', _:Date.now()}, {
      "simple": {},
      "dump": {params: {dump: "true"}},
      "liveNodes": {params: {path: '/live_nodes'}},
      "clusterState": {params: {detail: "true", path: "/clusterstate.json"}},
      "detail": {params: {detail: "true", path: "@path"}}
    });
  }])
.factory('Properties',
  ['$resource', function($resource) {
    return $resource('/solr/admin/info/properties', {'wt':'json', '_':Date.now()});
  }])
.factory('Threads',
  ['$resource', function($resource) {
    return $resource('/solr/admin/info/threads', {'wt':'json', '_':Date.now()});
  }])
.factory('Properties',
  ['$resource', function($resource) {
    return $resource('/solr/admin/info/properties', {'wt':'json', '_':Date.now()});
  }])
.factory('Replication',
  ['$resource', function($resource) {
    return $resource('/solr/:core/replication', {'wt':'json', core: "@core", 'command': 'details', '_':Date.now()}, {
      "details": {params: {command: "details"}}
    });
  }])
.factory('CoreSystem',
  ['$resource', function($resource) {
    return $resource('/solr/:core/admin/system', {wt:'json', core: "@core", _:Date.now()});
  }])
.factory('Update',
  ['$resource', function($resource) {
    return $resource('/solr/:core/:handler', {core: '@core', wt:'json', _:Date.now(), handler:'/update'}, {
      "optimize": {params: { optimize: "true"}},
      "commit": {params: {commit: "true"}},
      "post": {method: "POST", params: {handler: '@handler'}}
    });
  }])
.service('FileUpload', function ($http) {
    this.upload = function(params, file, success, error){
        var url = "/solr/" + params.core + "/" + params.handler + "?";
        raw = params.raw;
        delete params.core;
        delete params.handler;
        delete params.raw;
        url += $.param(params);
        if (raw && raw.length>0) {
            if (raw[0] != "&") raw = "&" + raw;
            url += raw;
        }
        var fd = new FormData();
        fd.append('file', file);
        $http.post(url, fd, {
            transformRequest: angular.identity,
            headers: {'Content-Type': undefined}
        }).success(success).error(error);
    }
})
.factory('Luke',
  ['$resource', function($resource) {
    return $resource('/solr/:core/admin/luke', {core: '@core', wt:'json', _:Date.now()}, {
      "schema": {params: {show:'schema'}},
      "index":  {params: {show:'index', numTerms: 0}}
    });
  }])
.factory('Analysis',
  ['$resource', function($resource) {
    return $resource('/solr/:core/analysis/field', {core: '@core', wt:'json', _:Date.now()}, {
      "field": {params: {"analysis.showmatch": true}}
    });
  }])
.factory('Ping',
  ['$resource', function($resource) {
    return $resource('/solr/:core/admin/ping', {wt:'json', core: '@core', ts:Date.now(), _:Date.now()}, {
     "ping": {},
     "status": {params:{action:"status"}}
    });
  }])
.factory('Mbeans',
  ['$resource', function($resource) {
    return $resource('/solr/:core/admin/mbeans', {'wt':'json', 'stats': true, '_':Date.now()}); // @core
  }])
.factory('Files',
  ['$resource', function($resource) {
    return $resource('/solr/:core/admin/file', {'wt':'json', core: '@core', '_':Date.now()}, {
      "list": {},
      "get": {method: "GET", interceptor: {
          response: function(config) {return config;}
      }}
    });
  }])
.factory('Query', // use $http for Query, as we need complete control over the URL
  ['$http', '$location', function($http, $location) {
    return {
      "query": function(url, callback) {
        $http({
          url:url,
          method: 'GET',
          transformResponse: [ function(data, headersGetter){ return {data:data}}]
        }).success(callback);
      }
    }}
]);
/*
http://localhost:8983/solr/techproducts/admin/mbeans?cat=QUERYHANDLER&wt=json&_=1419614354276
PING:
http://localhost:8983/solr/techproducts/admin/ping?wt=json&ts=1419614393324&_=1419614393325
*/


