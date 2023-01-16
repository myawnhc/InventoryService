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
import com.hazelcast.spi.exception.RetryableHazelcastException;
import org.hazelcast.msfdemo.invsvc.domain.Inventory;
import org.hazelcast.msfdemo.invsvc.domain.InventoryKey;

public class InventoryDAO {

    private HazelcastInstance hazelcast;
    private IMap<InventoryKey,Inventory> inventoryMap;

    private final String CREATE_MAPPING  =
            """
            CREATE MAPPING IF NOT EXISTS inventory_VIEW
            TYPE IMap
            Options (
                'keyFormat' = 'java',
                'keyJavaClass' = 'org.hazelcast.msfdemo.invsvc.domain.InventoryKey',
                'valueFormat' = 'java',
                'valueJavaClass' = 'org.hazelcast.msfdemo.invsvc.domain.Inventory'
            )
            """;

    public InventoryDAO(HazelcastInstance hz) {
        this.hazelcast = hz;
        inventoryMap = hz.getMap("inventory_VIEW");
        hazelcast.getSql().execute(CREATE_MAPPING);
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
        return inventoryMap.get(key);
    }

    public void insert(InventoryKey key, Inventory value) {
        inventoryMap.put(key, value);
    }

}