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
    return $resource('admin/info/system', {"wt":"json", "_":Date.now()});
  }])
.factory('Collections',
  ['$resource', function($resource) {
    return $resource('admin/collections',
    {'wt':'json', '_':Date.now()}, {
    "list": {params:{action: "LIST"}},
    "status": {params:{action: "CLUSTERSTATUS"}},
    "add": {params:{action: "CREATE"}},
    "delete": {params:{action: "DELETE"}},
    "rename": {params:{action: "RENAME"}},
    "createAlias": {params:{action: "CREATEALIAS"}},
    "deleteAlias": {params:{action: "DELETEALIAS"}},
    "deleteReplica": {params:{action: "DELETEREPLICA"}},
    "addReplica": {params:{action: "ADDREPLICA"}},
    "deleteShard": {params:{action: "DELETESHARD"}},
    "reload": {method: "GET", params:{action:"RELOAD", core: "@core"}}
    });
  }])
.factory('Cores',
  ['$resource', function($resource) {
    return $resource('admin/cores',
    {'wt':'json', '_':Date.now()}, {
    "query": {},
    "list": {params:{indexInfo: false}},
    "add": {params:{action: "CREATE"}},
    "unload": {params:{action: "UNLOAD", core: "@core"}},
    "rename": {params:{action: "RENAME"}},
    "swap": {params:{action: "SWAP"}},
    "reload": {method: "GET", params:{action:"RELOAD", core: "@core"}, headers:{doNotIntercept: "true"}}
    });
  }])
.factory('Logging',
  ['$resource', function($resource) {
    return $resource('admin/info/logging', {'wt':'json', '_':Date.now()}, {
      "events": {params: {since:'0'}},
      "levels": {},
      "setLevel": {}
      });
  }])
.factory('Zookeeper',
  ['$resource', function($resource) {
    return $resource('admin/zookeeper', {wt:'json', _:Date.now()}, {
      "simple": {},
      "dump": {params: {dump: "true"}},
      "liveNodes": {params: {path: '/live_nodes'}},
      "clusterState": {params: {detail: "true", path: "/clusterstate.json"}},
      "detail": {params: {detail: "true", path: "@path"}},
      "configs": {params: {detail:false, path: "/configs/"}},
      "aliases": {params: {detail: "true", path: "/aliases.json"}, transformResponse:function(data) {
        var znode = $.parseJSON(data).znode;
        if (znode.data) {
          return {aliases: $.parseJSON(znode.data).collection};
        } else {
          return {aliases: {}};
        }
      }}
    });
  }])
.factory('Properties',
  ['$resource', function($resource) {
    return $resource('admin/info/properties', {'wt':'json', '_':Date.now()});
  }])
.factory('Threads',
  ['$resource', function($resource) {
    return $resource('admin/info/threads', {'wt':'json', '_':Date.now()});
  }])
.factory('Properties',
  ['$resource', function($resource) {
    return $resource('admin/info/properties', {'wt':'json', '_':Date.now()});
  }])
.factory('Replication',
  ['$resource', function($resource) {
    return $resource(':core/replication', {'wt':'json', core: "@core", '_':Date.now()}, {
      "details": {params: {command: "details"}},
      "command": {params: {}}
    });
  }])
.factory('CoreSystem',
  ['$resource', function($resource) {
    return $resource(':core/admin/system', {wt:'json', core: "@core", _:Date.now()});
  }])
.factory('Update',
  ['$resource', function($resource) {
    return $resource(':core/:handler', {core: '@core', wt:'json', _:Date.now(), handler:'update'}, {
      "commit": {params: {commit: "true"}},
      "post": {headers: {'Content-type': 'application/json'}, method: "POST", params: {handler: '@handler'}},
      "postJson": {headers: {'Content-type': 'application/json'}, method: "POST", params: {handler: '@handler'}},
      "postXml": {headers: {'Content-type': 'text/xml'}, method: "POST", params: {handler: '@handler'}},
      "postCsv": {headers: {'Content-type': 'application/csv'}, method: "POST", params: {handler: '@handler'}}
    });
  }])
.service('FileUpload', function ($http) {
    this.upload = function(params, file, success, error){
        var url = "" + params.core + "/" + params.handler + "?";
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
    return $resource(':core/admin/luke', {core: '@core', wt:'json', _:Date.now()}, {
      "index":  {params: {numTerms: 0, show: 'index'}},
      "raw": {params: {numTerms: 0}},
      "schema": {params: {show:'schema'}},
      "field": {},
      "fields": {params: {show:'schema'}, interceptor: {
          response: function(response) {
              var fieldsAndTypes = [];
              for (var field in response.data.schema.fields) {
                fieldsAndTypes.push({group: "Fields", label: field, value: "fieldname=" + field});
              }
              for (var type in response.data.schema.types) {
                fieldsAndTypes.push({group: "Types", label: type, value: "fieldtype=" + type});
              }
              return fieldsAndTypes;
          }
      }}
    });
  }])
.factory('Analysis',
  ['$resource', function($resource) {
    return $resource(':core/analysis/field', {core: '@core', wt:'json', _:Date.now()}, {
      "field": {params: {"analysis.showmatch": true}}
    });
  }])
.factory('DataImport',
  ['$resource', function($resource) {
    return $resource(':core/:name', {core: '@core', name: '@name', indent:'on', wt:'json', _:Date.now()}, {
      "config": {params: {command: "show-config"}, headers: {doNotIntercept: "true"},
                 transformResponse: function(data) {
                    return {config: data};
                 }
                },
      "status": {params: {command: "status"}, headers: {doNotIntercept: "true"}},
      "reload": {params: {command: "reload-config"}},
      "post": {method: "POST",
                headers: {'Content-type': 'application/x-www-form-urlencoded'},
                transformRequest: function(data) { return $.param(data) }}
    });
  }])
.factory('Ping',
  ['$resource', function($resource) {
    return $resource(':core/admin/ping', {wt:'json', core: '@core', ts:Date.now(), _:Date.now()}, {
     "ping": {},
     "status": {params:{action:"status"}, headers: {doNotIntercept: "true"}
    }});
  }])
.factory('Mbeans',
  ['$resource', function($resource) {
    return $resource(':core/admin/mbeans', {'wt':'json', core: '@core', '_':Date.now()}, {
        stats: {params: {stats: true}},
        info: {},
        reference: {
            params: {wt: "xml", stats: true}, transformResponse: function (data) {
                return {reference: data}
            }
        },
        delta: {method: "POST",
                params: {stats: true, diff:true},
                headers: {'Content-type': 'application/x-www-form-urlencoded'},
                transformRequest: function(data) {
                    return "stream.body=" + encodeURIComponent(data);
                }
        }
    });
  }])
.factory('Files',
  ['$resource', function($resource) {
    return $resource(':core/admin/file', {'wt':'json', core: '@core', '_':Date.now()}, {
      "list": {},
      "get": {method: "GET", interceptor: {
          response: function(config) {return config;}
      }, transformResponse: function(data) {
          return data;
      }}
    });
  }])
.factory('Query',
    ['$resource', function($resource) {
       var resource = $resource(':core/:handler', {core: '@core', handler: '@handler', '_':Date.now()}, {
           "query": {
             method: "GET",
             transformResponse: function (data) {
               return {data: data}
             },
             headers: {doNotIntercept: "true"}
           }
       });
       resource.url = function(params) {
           var qs = [];
           for (key in params) {
               if (key != "core" && key != "handler") {
                   for (var i in params[key]) {
                       qs.push(key + "=" + params[key][i]);
                   }
               }
           }
           return "" + params.core + "/" + params.handler + "?" + qs.sort().join("&");
       }
       return resource;
}])
.factory('Segments',
   ['$resource', function($resource) {
       return $resource(':core/admin/segments', {'wt':'json', core: '@core', _:Date.now()}, {
           get: {}
       });
}])
.factory('Schema',
   ['$resource', function($resource) {
     return $resource(':core/schema', {wt: 'json', core: '@core', _:Date.now()}, {
       get: {method: "GET"},
       check: {method: "GET", headers: {doNotIntercept: "true"}},
       post: {method: "POST"}
     });
}])
.factory('Config',
   ['$resource', function($resource) {
     return $resource(':core/config', {wt: 'json', core: '@core', _:Date.now()}, {
       get: {method: "GET"}
     })
}]);
