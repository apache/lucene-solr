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

solrAdminApp.controller('PluginsController',
    function($scope, $rootScope, $routeParams, $location, Mbeans, Constants) {
        $scope.resetMenu("plugins", Constants.IS_CORE_PAGE);

        if ($routeParams.legacytype) {
            // support legacy URLs. Angular cannot change #path without reloading controller
            $location.path("/"+$routeParams.core+"/plugins");
            $location.search("type", $routeParams.legacytype);
            return;
        }

        $scope.refresh = function() {
            Mbeans.stats({core: $routeParams.core}, function (data) {
                var type = $location.search().type;
                $scope.types = getPluginTypes(data, type);
                $scope.type = getSelectedType($scope.types, type);

                if ($scope.type && $routeParams.entry) {
                    $scope.plugins = $routeParams.entry.split(",");
                    openPlugins($scope.type, $scope.plugins);
                } else {
                    $scope.plugins = [];
                }
            });
        };

        $scope.selectPluginType = function(type) {
            $location.search({entry:null, type: type.lower});
            $scope.type = type;
        };

        $scope.selectPlugin = function(plugin) {
            plugin.open = !plugin.open;

            if (plugin.open) {
                $scope.plugins.push(plugin.name);
            } else {
                $scope.plugins.splice($scope.plugins.indexOf(plugin.name), 1);
            }

            if ($scope.plugins.length==0) {
                $location.search("entry", null);
            } else {
                $location.search("entry", $scope.plugins.join(','));
            }
        }

        $scope.startRecording = function() {
            $scope.isRecording = true;
            Mbeans.reference({core: $routeParams.core}, function(data) {
                $scope.reference = data.reference;
                console.log($scope.reference);
            })
        }

        $scope.stopRecording = function() {
            $scope.isRecording = false;
            console.log($scope.reference);
            Mbeans.delta({core: $routeParams.core}, $scope.reference, function(data) {
                parseDelta($scope.types, data);
            });
        }

        $scope.refresh();
    });

var getPluginTypes = function(data, selected) {
    var keys = [];
    var mbeans = data["solr-mbeans"];
    for (var i=0; i<mbeans.length; i+=2) {
        var key = mbeans[i];
        var lower = key.toLowerCase();
        var plugins = getPlugins(mbeans[i+1]);
        if (plugins.length == 0) continue;
        keys.push({name: key,
                   selected: lower == selected,
                   changes: 0,
                   lower: lower,
                   plugins: plugins
        });
    }
    keys.sort(function(a,b) {return a.name > b.name});
    return keys;
};

var getPlugins = function(data) {
    var plugins = [];
    for (var key in data) {
        var pluginProperties = data[key];
        var stats = pluginProperties.stats;
        delete pluginProperties.stats;
        for (var stat in stats) {
            // add breaking space after a bracket or @ to handle wrap long lines:
            stats[stat] = new String(stats[stat]).replace( /([\(@])/g, '$1&#8203;');
        }
        plugin = {name: key, changed: false, stats: stats, open:false};
        plugin.properties = pluginProperties;
        plugins.push(plugin);
    }
    plugins.sort(function(a,b) {return a.name > b.name});
    return plugins;
};

var getSelectedType = function(types, selected) {
    if (selected) {
        for (var i in types) {
            if (types[i].lower == selected) {
                return types[i];
            }
        }
    }
};

var parseDelta = function(types, data) {

    var getByName = function(list, name) {
        for (var i in list) {
            if (list[i].name == name) return list[i];
        }
    }

    var mbeans = data["solr-mbeans"]
    for (var i=0; i<mbeans.length; i+=2) {
        var typeName = mbeans[i];
        var type = getByName(types, typeName);
        var plugins = mbeans[i+1];
        for (var key in plugins) {
            var changedPlugin = plugins[key];
            if (changedPlugin._changed_) {
                var plugin = getByName(type.plugins, key);
                var stats = changedPlugin.stats;
                delete changedPlugin.stats;
                plugin.properties = changedPlugin;
                for (var stat in stats) {
                    // add breaking space after a bracket or @ to handle wrap long lines:
                    plugin.stats[stat] = new String(stats[stat]).replace( /([\(@])/g, '$1&#8203;');
                }
                plugin.changed = true;
                type.changes++;
            }
        }
    }
};

var openPlugins = function(type, selected) {
    for (var i in type.plugins) {
        var plugin = type.plugins[i];
        plugin.open = selected.indexOf(plugin.name)>=0;
    }
}
