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
import org.hazelcast.msfdemo.invsvc.domain.Item;

public class ItemDAO  {

    private HazelcastInstance hazelcast;
    private IMap<String, Item> itemMap;

    private final String CREATE_MAPPING  =
            """
            CREATE MAPPING IF NOT EXISTS item_VIEW
            TYPE IMap
            Options (
                'keyFormat' = 'java',
                'keyJavaClass' = 'java.lang.String',
                'valueFormat' = 'java',
                'valueJavaClass' = 'org.hazelcast.msfdemo.invsvc.domain.Item'
            )
            """;

    public ItemDAO(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
       itemMap = hazelcast.getMap("item_VIEW");
        hazelcast.getSql().execute(CREATE_MAPPING);
    }

    public int getItemCount() {
        return itemMap.size();
    }

    public void deleteAll() {
        itemMap.clear();
    }

    public Item findByKey(String itemNumber) {
        return itemMap.get(itemNumber);
    }

    public void insert(String itemNumber, Item value) {
        itemMap.put(itemNumber, value);
    }
}
