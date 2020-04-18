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

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FontAwesomeModule, FaIconLibrary } from '@fortawesome/angular-fontawesome';
import {
    faCoffee,
    faTachometerAlt,
    faFile,
    faCloud,
    faBoxes,
    faBars,
    faPoop,
    faInfoCircle
} from '@fortawesome/free-solid-svg-icons';
import { faJava } from '@fortawesome/free-brands-svg-icons';
@NgModule({
    declarations: [],
    imports: [
        CommonModule,
        FontAwesomeModule
    ]
})
export class IconsModule {
    constructor(library:FaIconLibrary) {
        // Add an icon to the library for convenient access in other components
        library.addIcons(faCoffee);
        library.addIcons(faTachometerAlt);
        library.addIcons(faFile);
        library.addIcons(faCloud);
        library.addIcons(faCoffee);
        library.addIcons(faBoxes);
        library.addIcons(faBars);
        library.addIcons(faPoop);
        library.addIcons(faInfoCircle);
        library.addIcons(faCoffee);
            // Brands
        library.addIcons(faJava);
    }
}
