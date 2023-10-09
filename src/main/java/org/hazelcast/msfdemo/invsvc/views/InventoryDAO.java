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

package org.hazelcast.msfdemo.invsvc.views;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import org.hazelcast.msfdemo.invsvc.domain.Inventory;
import org.hazelcast.msfdemo.invsvc.domain.InventoryKey;

public class InventoryDAO {

    private HazelcastInstance hazelcast;
    private IMap<GenericRecord,GenericRecord> inventoryMap;

    private final String CREATE_MAPPING  =
            """
                    CREATE MAPPING IF NOT EXISTS inventory_VIEW (
                        itemNumber          VARCHAR,
                        description         VARCHAR,
                        location            VARCHAR,
                        locationType        VARCHAR,
                        geohash             VARCHAR,
                        quantityOnHand      INTEGER,
                        quantityReserved    INTEGER,
                        availableToPromise  INTEGER
                    )
                    TYPE IMap
                    Options (
                        'keyFormat' = 'compact',
                        'keyCompactTypeName' = 'InventoryService.inventoryKey',
                        'valueFormat' = 'compact',
                        'valueCompactTypeName' = 'InventoryService.inventory'
                    )
                    """;

    public InventoryDAO(HazelcastInstance hz) {
        this.hazelcast = hz;
        inventoryMap = hz.getMap("inventory_VIEW");
        try {
            hazelcast.getSql().execute(CREATE_MAPPING);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Non-inheritable query methods
    public int getInventoryRecordCount() {
        boolean logged = false;
        while (true) {
            try {
                return inventoryMap.size();
            } catch (RetryableHazelcastException rhe) { // probably caught before it reaches us, but let's try ...
                if (!logged) {
                    System.out.println("InventoryDAO.getInventoryRecordCount blocked until data load completes.");
                    logged = true;
                }
            }
        }
    }

    public void deleteAll() {
        inventoryMap.clear();
    }

    public Inventory findByKey(InventoryKey key) {
        GenericRecord keyGr = key.toGenericRecord();
        GenericRecord valueGR = inventoryMap.get(keyGr);
        if (valueGR == null) {
            System.out.println("InventoryDAO.findByKey: No inventory record found for key " + key);
            return null;
        }
        return new Inventory(valueGR);
        //return new Inventory(inventoryMap.get(key.toGenericRecord()));
    }

    public void insert(InventoryKey key, Inventory value) {
        inventoryMap.put(key.toGenericRecord(), value.toGenericRecord());
    }

}