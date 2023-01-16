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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.msfdemo.invsvc.domain.Inventory;
import org.hazelcast.msfdemo.invsvc.domain.InventoryKey;

import java.io.Serializable;

// TODO: will make Compact Serializable
public class ReserveInventoryEvent extends InventoryEvent implements Serializable {
//    private String orderNumber = "[not provided]";
//    private String locationID;
//    private int quantity;

    public ReserveInventoryEvent(InventoryKey key, int qty) {
        this.key = key;
        this.eventClass = ReserveInventoryEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put("itemNumber", key.itemNumber);
        jobj.put("location", key.locationID);
        jobj.put("quantity", qty);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public ReserveInventoryEvent(SqlRow row) {
        this.key = row.getObject("key");
        eventClass = ReserveInventoryEvent.class.getCanonicalName();
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        setTimestamp(row.getObject("timestamp"));
    }

    @Override
    public Inventory apply(Inventory inventory) {
        JSONObject jobj = new JSONObject(payload.getValue());
        inventory.setItemNumber(key.itemNumber);
        inventory.setLocation(key.locationID);
        inventory.setQuantityReserved(inventory.getQuantityReserved() + jobj.getInt("quantity"));
        return inventory;
    }
}
