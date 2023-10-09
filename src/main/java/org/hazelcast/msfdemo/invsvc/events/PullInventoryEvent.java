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

package org.hazelcast.msfdemo.invsvc.events;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.msfdemo.invsvc.domain.Inventory;
import org.hazelcast.msfdemo.invsvc.domain.InventoryKey;

public class PullInventoryEvent extends InventoryEvent  {

    public static final String QUAL_EVENT_NAME = "InventoryService.PullInventoryEvent";
    public static final String INVENTORY_KEY = "key";
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String LOCATION = "location";
    public static final String QUANTITY = "quantity";

    //private InventoryKey key; // itemNumber on base class - keep components or not?
    private String itemNumber;
    private String location;
    private int quantity;

    public PullInventoryEvent(InventoryKey inventoryKey, int quantity) {
        setEventName(QUAL_EVENT_NAME);
        super.key = inventoryKey;
        this.itemNumber = key.itemNumber;
        this.location = key.locationID;
        this.quantity = quantity;
    }

    public PullInventoryEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.itemNumber = data.getString(ITEM_NUMBER);
        this.location = data.getString(LOCATION);
        super.key = new InventoryKey(itemNumber, location);
        this.quantity = data.getInt32(QUANTITY);
        Long time = data.getInt64(EVENT_TIME);
        if (time != null)
            setTimestamp(time);
    }

    public PullInventoryEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        InventoryKey key = row.getObject("doKey");
        super.key = key;
        this.itemNumber = key.itemNumber;
        this.location = key.locationID;
        this.quantity = row.getObject(QUANTITY);
        Long time = row.getObject(EVENT_TIME);
        if (time != null)
            setTimestamp(time);
    }

    @Override
    public Inventory apply(Inventory inventory) {
        inventory.setItemNumber(key.itemNumber);
        inventory.setLocation(key.locationID);
        inventory.setQuantityReserved(inventory.getQuantityReserved() - quantity);
        inventory.setQuantityOnHand(inventory.getQuantityOnHand() - quantity);
        inventory.setAvailableToPromise(inventory.getAvailableToPromise() - quantity);
        return inventory;
    }

    @Override
    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(getEventName())
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setInt64(EVENT_TIME, timestamp)
                .setGenericRecord(INVENTORY_KEY, super.key.toGenericRecord())
                .setString(ITEM_NUMBER, itemNumber)
                .setString(LOCATION, location)
                .setInt32(QUANTITY, quantity)
                .build();
        return gr;
    }
}
