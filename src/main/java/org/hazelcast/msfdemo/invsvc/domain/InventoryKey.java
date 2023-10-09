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

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import org.hazelcast.eventsourcing.event.GenericRecordSupport;

import java.io.Serializable;

// When stored in as key to materialized view, will be stored as a GenericRecord in
// compact format; but needs to be Java Serializable when used as part of the
// PartitionedSequenceKey of the EventStore

public class InventoryKey implements Comparable<InventoryKey>, GenericRecordSupport, Serializable {
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String LOCATION_ID = "locationID";

    public String itemNumber;
    public String locationID;

    public InventoryKey(String item, String loc) {
        this.itemNumber = item;
        this.locationID = loc;
    }

    public InventoryKey(GenericRecord data) {
        this.itemNumber = data.getString(ITEM_NUMBER);
        this.locationID = data.getString(LOCATION_ID);
    }

    @Override
    public String toString() {
        return itemNumber + " @ " + locationID;
    }

    @Override
    public int compareTo(InventoryKey o) {
        if (o instanceof InventoryKey) {
            InventoryKey other = (InventoryKey)  o;
            if (! o.itemNumber.equals(this.itemNumber))
                return this.itemNumber.compareTo(o.itemNumber);
            else
                return this.locationID.compareTo(o.locationID);
        };
        return -1; // meaningless comparison
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof InventoryKey) {
            InventoryKey other = (InventoryKey) o;
            return itemNumber.equals(other.itemNumber) && locationID.equals(other.locationID);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return itemNumber.hashCode() + locationID.hashCode();
    }

    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact("InventoryService.inventoryKey")
                .setString(ITEM_NUMBER, itemNumber)
                .setString(LOCATION_ID, locationID)
                .build();
        return gr;
    }
}
