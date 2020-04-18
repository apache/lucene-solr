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
