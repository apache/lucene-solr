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

var contentTypeMap = { xml : 'text/xml', html : 'text/html', js : 'text/javascript', json : 'application/json', 'css' : 'text/css' };
var languages = {js: "javascript", xml:"xml", xsl:"xml", vm: "xml", html: "xml", json: "json", css: "css"};

solrAdminApp.controller('FilesController',
    function($scope, $rootScope, $routeParams, $location, Files, Constants) {
        $scope.resetMenu("files", Constants.IS_COLLECTION_PAGE);

        $scope.file = $location.search().file;
        $scope.content = null;

        $scope.baseurl = $location.absUrl().substr(0,$location.absUrl().indexOf("#")); // Including /solr/ context

        $scope.refresh = function () {

            var process = function (path, tree) {
                var params = {core: $routeParams.core};
                if (path.slice(-1) == '/') {
                    params.file = path.slice(0, -1);
                } else if (path!='') {
                    params.file = path;
                }

                Files.list(params, function (data) {
                    var filenames = Object.keys(data.files);
                    filenames.sort();
                    for (var i in filenames) {
                        var file = filenames[i];
                        var filedata = data.files[file];
                        var state = undefined;
                        var children = undefined;

                        if (filedata.directory) {
                            file = file + "/";
                            if ($scope.file && $scope.file.indexOf(path + file) == 0) {
                                state = "open";
                            } else {
                                state = "closed";
                            }
                            children = [];
                            process(path + file, children);
                        }
                        tree.push({
                            data: {
                                title: file,
                                attr: { id: path + file}
                            },
                            children: children,
                            state: state
                        });
                    }
                });
            }
            $scope.tree = [];
            process("", $scope.tree);

            if ($scope.file && $scope.file != '' && $scope.file.split('').pop()!='/') {
                var extension;
                if ($scope.file == "managed-schema") {
                  extension = contentTypeMap['xml'];
                } else {
                  extension = $scope.file.match( /\.(\w+)$/)[1] || '';
                }
                var contentType = (contentTypeMap[extension] || 'text/plain' ) + ';charset=utf-8';

                Files.get({core: $routeParams.core, file: $scope.file, contentType: contentType}, function(data) {
                    $scope.content = data.data;
                    $scope.url = data.config.url + "?" + $.param(data.config.params);  // relative URL
                    if (contentType.indexOf("text/plain") && (data.data.indexOf("<?xml")>=0) || data.data.indexOf("<!--")>=0) {
                        $scope.lang = "xml";
                    } else {
                        $scope.lang = languages[extension] || "txt";
                    }
                });
            }
        };

        $scope.showTreeLink = function(data) {
            var file = data.args[0].id;
            $location.search({file:file});
        };

        $scope.refresh();
    });
