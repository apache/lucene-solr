package org.apache.lucene.swing.models;


/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Jonathan Simon - jonathan_s_simon@yahoo.com
 */
public class DataStore {

    private static final String ITALIAN_CATEGORY = "Italian";
    private static final String CUBAN_CATEGORY = "Cuban";
    private static final String STEAK_CATEGORY = "Steak";
    private static int id = 0;

    static Collection restaurants = new ArrayList();
    static RestaurantInfo pinos = new RestaurantInfo();
    static RestaurantInfo canolis = new RestaurantInfo();
    static RestaurantInfo picadillo = new RestaurantInfo();
    static RestaurantInfo versailles = new RestaurantInfo();
    static RestaurantInfo laCaretta = new RestaurantInfo();
    static RestaurantInfo laCaretta2 = new RestaurantInfo();
    static RestaurantInfo laCaretta3 = new RestaurantInfo();
    static RestaurantInfo ranchaLuna = new RestaurantInfo();
    static RestaurantInfo leMerais = new RestaurantInfo();
    static RestaurantInfo chris = new RestaurantInfo();
    static RestaurantInfo outback = new RestaurantInfo();
    static RestaurantInfo outback2 = new RestaurantInfo();
    static RestaurantInfo outback3 = new RestaurantInfo();
    static RestaurantInfo outback4 = new RestaurantInfo();


    public static Iterator getRestaurants(){
        return restaurants.iterator();
    }

    static {
        pinos.setId(getNextId());
        pinos.setType(ITALIAN_CATEGORY);
        pinos.setName("Pino's");
        pinos.setPhone("(305) 111-2222");
        pinos.setStreet("12115 105th Street ");
        pinos.setCity("Miami");
        pinos.setState("FL");
        pinos.setZip("33176");
        restaurants.add(pinos);

        canolis.setId(getNextId());
        canolis.setType(ITALIAN_CATEGORY);
        canolis.setName("Canoli's");
        canolis.setPhone("(305) 234-5543");
        canolis.setStreet("12123 85th Street ");
        canolis.setCity("Miami");
        canolis.setState("FL");
        canolis.setZip("33176");
        restaurants.add(canolis);

        picadillo.setId(getNextId());
        picadillo.setType(CUBAN_CATEGORY);
        picadillo.setName("Picadillo");
        picadillo.setPhone("(305) 746-7865");
        picadillo.setStreet("109 12th Street ");
        picadillo.setCity("Miami");
        picadillo.setState("FL");
        picadillo.setZip("33176");
        restaurants.add(picadillo);

        versailles.setId(getNextId());
        versailles.setType(CUBAN_CATEGORY);
        versailles.setName("Cafe Versailles");
        versailles.setPhone("(305) 201-5438");
        versailles.setStreet("312 8th Street ");
        versailles.setCity("Miami");
        versailles.setState("FL");
        versailles.setZip("33176");
        restaurants.add(versailles);

        laCaretta.setId(getNextId());
        laCaretta.setType(CUBAN_CATEGORY);
        laCaretta.setName("La Carretta");
        laCaretta.setPhone("(305) 342-9876");
        laCaretta.setStreet("348 8th Street ");
        laCaretta.setCity("Miami");
        laCaretta.setState("FL");
        laCaretta.setZip("33176");
        restaurants.add(laCaretta);

        laCaretta2.setId(getNextId());
        laCaretta2.setType(CUBAN_CATEGORY);
        laCaretta2.setName("La Carretta");
        laCaretta2.setPhone("(305) 556-9876");
        laCaretta2.setStreet("31224 23rd Street ");
        laCaretta2.setCity("Miami");
        laCaretta2.setState("FL");
        laCaretta2.setZip("33176");
        restaurants.add(laCaretta2);

        laCaretta3.setId(getNextId());
        laCaretta3.setType(CUBAN_CATEGORY);
        laCaretta3.setName("La Carretta");
        laCaretta3.setPhone("(305) 682-9876");
        laCaretta3.setStreet("23543 107th Street ");
        laCaretta3.setCity("Miami");
        laCaretta3.setState("FL");
        laCaretta3.setZip("33176");
        restaurants.add(laCaretta3);

        ranchaLuna.setId(getNextId());
        ranchaLuna.setType(CUBAN_CATEGORY);
        ranchaLuna.setName("Rancha Luna");
        ranchaLuna.setPhone("(305) 777-4384");
        ranchaLuna.setStreet("110 23rd Street ");
        ranchaLuna.setCity("Miami");
        ranchaLuna.setState("FL");
        ranchaLuna.setZip("33176");
        restaurants.add(ranchaLuna);

        leMerais.setId(getNextId());
        leMerais.setType(STEAK_CATEGORY);
        leMerais.setName("Le Merais");
        leMerais.setPhone("(212) 654-9187");
        leMerais.setStreet("11 West 46th Street");
        leMerais.setCity("New York");
        leMerais.setState("NY");
        leMerais.setZip("10018");
        restaurants.add(leMerais);

        chris.setId(getNextId());
        chris.setType(STEAK_CATEGORY);
        chris.setName("Ruth's Chris Seakhouse");
        chris.setPhone("(305) 354-8885");
        chris.setStreet("12365 203rd Street ");
        chris.setCity("Miami");
        chris.setState("FL");
        chris.setZip("33176");
        restaurants.add(chris);

        outback.setId(getNextId());
        outback.setType(STEAK_CATEGORY);
        outback.setName("Outback");
        outback.setPhone("(305) 244-7623");
        outback.setStreet("348 136th Street ");
        outback.setCity("Miami");
        outback.setState("FL");
        outback.setZip("33176");
        restaurants.add(outback);

        outback2.setId(getNextId());
        outback2.setType(STEAK_CATEGORY);
        outback2.setName("Outback");
        outback2.setPhone("(305) 533-6522");
        outback2.setStreet("21 207th Street ");
        outback2.setCity("Miami");
        outback2.setState("FL");
        outback2.setZip("33176");
        restaurants.add(outback2);

        outback3.setId(getNextId());
        outback3.setType(STEAK_CATEGORY);
        outback3.setName("Outback");
        outback3.setPhone("(305) 244-7623");
        outback3.setStreet("10117 107th Street ");
        outback3.setCity("Miami");
        outback3.setState("FL");
        outback3.setZip("33176");
        restaurants.add(outback3);

        outback4.setId(getNextId());
        outback4.setType(STEAK_CATEGORY);
        outback4.setName("Outback");
        outback4.setPhone("(954) 221-3312");
        outback4.setStreet("10 11th Street ");
        outback4.setCity("Aventura");
        outback4.setState("FL");
        outback4.setZip("32154");
        restaurants.add(outback4);

    }

    private static int getNextId(){
        id++;
        return id;
    }

}
