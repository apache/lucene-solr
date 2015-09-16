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
//helper for formatting JSON and others

var DOC_PLACEHOLDER = '<doc>\n' +
                '<field name="id">change.me</field>' +
                '<field name="title">change.me</field>' +
                '</doc>';

var ADD_PLACEHOLDER = '<add>\n' + DOC_PLACEHOLDER + '</add>\n';

solrAdminApp.controller('DocumentsController',
    function($scope, $rootScope, $routeParams, $location, Luke, Update, FileUpload, Constants) {
        $scope.resetMenu("documents", Constants.IS_COLLECTION_PAGE);

        $scope.refresh = function () {
            Luke.schema({core: $routeParams.core}, function(data) {
                //TODO: handle dynamic fields
                delete data.schema.fields._version_;
                $scope.fields = Object.keys(data.schema.fields);
            });
            $scope.document = "";
            $scope.handler = "/update";
            $scope.type = "json";
            $scope.commitWithin = 1000;
            $scope.overwrite = true;
            $scope.boost = "1.0";
        };

        $scope.refresh();

        $scope.changeDocumentType = function () {
            $scope.placeholder = "";
            if ($scope.type == 'json') {
                $scope.placeholder = '{"id":"change.me","title":"change.me"}';
            } else if ($scope.type == 'csv') {
                $scope.placeholder = "id,title\nchange.me,change.me";
            } else if ($scope.type == 'solr') {
                $scope.placeholder = ADD_PLACEHOLDER;
            } else if ($scope.type == 'xml') {
                $scope.placeholder = DOC_PLACEHOLDER;
            }
        };

        $scope.addWizardField = function () {
            if ($scope.document == "") $scope.document = "{}";
            var doc = JSON.parse($scope.document);
            doc[$scope.fieldName] = $scope.fieldData;
            $scope.document = JSON.stringify(doc, null, '\t');
            $scope.fieldData = "";
        };

        $scope.submit = function () {
            var contentType = "";
            var postData = "";
            var params = {};
            var doingFileUpload = false;

            if ($scope.handler[0] == '/') {
                params.handler = $scope.handler.substring(1);
            } else {
                params.handler = 'update';
                params.qt = $scope.handler;
            }

            params.commitWithin = $scope.commitWithin;
            params.boost = $scope.boost;
            params.overwrite = $scope.overwrite;
            params.core = $routeParams.core;
            params.wt = "json";

            if ($scope.type == "json" || $scope.type == "wizard") {
                postData = "[" + $scope.document + "]";
                contentType = "json";
            } else if ($scope.type == "csv") {
                postData = $scope.document;
                contentType = "csv";
            } else if ($scope.type == "xml") {
                postData = "<add>" + $scope.document + "</add>";
                contentType = "xml";
            } else if ($scope.type == "upload") {
                doingFileUpload = true;
                params.raw = $scope.literalParams;
            } else if ($scope.type == "solr") {
                postData = $scope.document;
                if (postData[0] == "<") {
                    contentType = "xml";
                } else if (postData[0] == "{" || postData[0] == '[') {
                    contentType = "json";
                } else {
                    alert("Cannot identify content type")
                }
            }
            if (!doingFileUpload) {
                var callback = function (success) {
                  $scope.responseStatus = "success";
                  delete success.$promise;
                  delete success.$resolved;
                  $scope.response = JSON.stringify(success, null, '  ');
                };
                var failure = function (failure) {
                    $scope.responseStatus = failure;
                };
                if (contentType == "json") {
                  Update.postJson(params, postData, callback, failure);
                } else if (contentType == "xml") {
                  Update.postXml(params, postData, callback, failure);
                } else if (contentType == "csv") {
                  Update.postCsv(params, postData, callback, failure);
                }
            } else {
                var file = $scope.fileUpload;
                console.log('file is ' + JSON.stringify(file));
                var uploadUrl = "/fileUpload";
                FileUpload.upload(params, $scope.fileUpload, function (success) {
                    $scope.responseStatus = "success";
                    $scope.response = JSON.stringify(success, null, '  ');
                }, function (failure) {
                    $scope.responseStatus = "failure";
                    $scope.response = JSON.stringify(failure, null, '  ');
                });
            }
        }
    });

