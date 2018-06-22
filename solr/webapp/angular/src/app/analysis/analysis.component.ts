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

import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { SharedService } from '../shared.service';
import { AnalysisService } from '../analysis.service';
import { LukeService } from '../luke.service';

@Component({
  selector: 'app-analysis',
  templateUrl: './analysis.component.html'
})
export class AnalysisComponent implements OnInit {
  sharedService: SharedService;
  isHandlerMissing = false;
  analysisError = null;
  indexText = null;
  queryText = null;
  fieldOrType = null;
  verbose = false;
  schemaBrowseUrl = null;

  collectionName: string;
  fieldsAndTypes: any[];
  result : any;
  resultKeys : any;

  constructor(private route: ActivatedRoute,
    sharedService: SharedService,
    private lukeService: LukeService,
    private analysisService: AnalysisService) {
    this.sharedService = sharedService;
  }

  refresh() {
    this.lukeService.schema(this.collectionName).subscribe(r => {
      this.fieldsAndTypes = r;
    });

  }

  ngOnInit() {
    this.route.params.subscribe(params => {
      this.collectionName = params['name'];
    });
    this.refresh();
  }

  toggleVerbose() {
    this.verbose = !this.verbose;
  }

  changeFieldOrType() {
    const parts = this.fieldOrType.split("=");
    this.schemaBrowseUrl = parts[0]=="fieldname" ? ("field=" + parts[1]) : ("type=" + parts[1]);
  }

  updateQueryString() {
    if (this.fieldOrType && (this.indexText || this.queryText)) {
      const parts = this.fieldOrType.split("=");
      const fieldname = parts[0] == "fieldname" ? parts[1] : null;
      const fieldtype = parts[0] == "fieldtype" ? parts[1] : null;
      this.analysisService.fieldAnalysis(this.collectionName, this.indexText, this.queryText, fieldname, fieldtype, this.verbose)
        .subscribe(r => {
          this.result = r;
          this.resultKeys = Object.keys(r);
        }, (error => {
          this.sharedService.clearErrors();
          this.sharedService.showError(error.error);
        }));
    }
  }
}
