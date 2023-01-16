/*
 * Copyright 2018-2022 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hazelcast.msfdemo.invsvc.domain;

import org.hazelcast.eventsourcing.event.DomainObject;

public class Inventory implements DomainObject<InventoryKey> {
    // From ITEM table
    private String itemNumber;
    private String description;
    // From LOCATION table
    private String location;
    private String locationType; // Enum?
    private String geohash;
    // From INVENTORY table
    private int    quantityOnHand;
    private int    quantityReserved;
    private int    availableToPromise;

    @Override
    public InventoryKey getKey() { return new InventoryKey(itemNumber, location); }

    public String getItemNumber() {
        return itemNumber;
    }

    public void setItemNumber(String itemNumber) {
        this.itemNumber = itemNumber;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLocationType() {
        return locationType;
    }

    public void setLocationType(String locationType) {
        this.locationType = locationType;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public int getQuantityOnHand() {
        return quantityOnHand;
    }

    public void setQuantityOnHand(int quantityOnHand) {
        this.quantityOnHand = quantityOnHand;
    }

    public int getQuantityReserved() {
        return quantityReserved;
    }

    public void setQuantityReserved(int quantityReserved) {
        this.quantityReserved = quantityReserved;
    }

    public int getAvailableToPromise() {
        return availableToPromise;
    }

    public void setAvailableToPromise(int availableToPromise) {
        this.availableToPromise = availableToPromise;
    }

    public String toString() {
        return itemNumber + " " + location + " " + " QOH " + quantityOnHand + " RSV " + quantityReserved + " ATP " + availableToPromise;
    }
}
