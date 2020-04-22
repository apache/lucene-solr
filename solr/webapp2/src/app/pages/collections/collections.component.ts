// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
import { Component, OnInit } from '@angular/core';
import { FormsModule, FormBuilder, FormGroup, FormControl, Validators } from '@angular/forms';
import { SolrCollectionsService } from '../../services/solr-collections/solr-collections.service';
import {trimTrailingNulls} from "@angular/compiler/src/render3/view/util";

@Component({
  selector: 'app-collections',
  templateUrl: './collections.component.html',
  styleUrls: ['./collections.component.scss']
})
export class CollectionsComponent implements OnInit {
    collectionNames;
    collectionName: string;
    formGroup: FormGroup;
    queryResults: string;
    params: any;

  constructor(private collectionsService: SolrCollectionsService, private formBuilder: FormBuilder) { }

  ngOnInit() {
      this.collectionsService.get().subscribe(response => {
        this.collectionNames = response.collections;
      });
    this.createForm();
  }
  createForm() {
    this.formGroup = this.formBuilder.group({
      'qt': [null],
      'q': [null, Validators.required],
      'fq': [null],
      'sort': [null],
      'start': [null],
      'rows': [null],
      'fl': [null],
      'df': [null],
      'queryParameters': [null],
      'wt': [null],
    });
  }
  get query() {
    return this.formGroup.get('q') as FormControl
  }
  setCollection(collectionName){
      console.log("Form working ish", this.collectionName);
      return this.collectionName;
  }
  onSubmit(params){
    // @ts-ignore
    let filteredObject = Object.entries(params).reduce((a,[k,v]) => (v == null ? a : {
        ...a,
        [k]:v}),
      {});

    let queryString = Object.keys(filteredObject).map(key => key + '=' + params[key]).join('&');
    this.collectionsService.getResults(this.collectionName, queryString).subscribe(response => {
      this.queryResults = JSON.stringify(response, undefined, 4);
      return this.queryResults;
    })
  }

}
