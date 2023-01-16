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

package org.hazelcast.msfdemo.invsvc.service;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import io.grpc.stub.StreamObserver;
import org.example.grpc.GrpcServer;
import org.hazelcast.eventsourcing.EventSourcingController;

import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.eventsourcing.sync.EventCompletionHandler;
import org.hazelcast.msfdemo.invsvc.business.InventoryAPIImpl;
import org.hazelcast.msfdemo.invsvc.config.ServiceConfig;
import org.hazelcast.msfdemo.invsvc.domain.Inventory;
import org.hazelcast.msfdemo.invsvc.domain.InventoryKey;
import org.hazelcast.msfdemo.invsvc.events.InventoryEvent;
import org.hazelcast.msfdemo.invsvc.events.PullInventoryEventSerializer;
import org.hazelcast.msfdemo.invsvc.events.ReserveInventoryEventSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class InventoryService {

    private HazelcastInstance hazelcast;
    private EventSourcingController<Inventory, InventoryKey, InventoryEvent> eventSourcingController;
    private boolean embedded;
    private byte[] clientConfig;

    final private Map<PartitionedSequenceKey, EventCompletionHandler<Inventory, InventoryKey, InventoryEvent>> awaitingCompletion;

    public InventoryService() {
        awaitingCompletion = new HashMap<>();
    }

    // For use by APIImpl to register callbacks to invoke when EventSourcing pipeline completes
    public void registerObserver(PartitionedSequenceKey key, EventCompletionHandler<Inventory, InventoryKey, InventoryEvent> callback) {
        awaitingCompletion.put(key, callback);
        System.out.println("InventoryService: Registered callback for " + key);

        // Race condition: Event may have already been processed, if so touch item to make trigger update listener
        String mapName = eventSourcingController.getCompletionMapName();
        IMap<PartitionedSequenceKey,CompletionInfo> completionsMap = hazelcast.getMap(mapName);
        CompletionInfo cinfo = completionsMap.get(key);
        System.out.println("   completion info state: " + cinfo.status);
        if (cinfo != null && cinfo.status != CompletionInfo.Status.INCOMPLETE) {
            cinfo.completionTime += 1; // update listener not triggered unless we modify something
            completionsMap.put(key, cinfo);
        }
    }

    private void initHazelcast(boolean isEmbedded, byte[] clientConfig) {
        this.embedded = isEmbedded;
        this.clientConfig = clientConfig;
        if (!embedded && clientConfig == null) {
            throw new IllegalArgumentException("ClientConfig cannot be null for client-server deployment");
        }
        if (embedded) {
            Config config = new Config();
            config.setClusterName("invsvc");
            config.getNetworkConfig().setPort(5721);
            config.getJetConfig().setEnabled(true);
            config.getMapConfig("inventory_PENDING").getEventJournalConfig().setEnabled(true);
            config.getSerializationConfig().getCompactSerializationConfig()
                    .addSerializer(new ReserveInventoryEventSerializer())
                    .addSerializer(new PullInventoryEventSerializer());
            hazelcast = Hazelcast.newHazelcastInstance(config);
        } else {
            throw new IllegalArgumentException("Not set up to handle client-server yet");
        }

        // Set AccountService for additional code needed for cloud deployment
    }

    private void initEventSourcingController(HazelcastInstance hazelcast) {
        eventSourcingController = EventSourcingController
                .<Inventory, InventoryKey, InventoryEvent>newBuilder(hazelcast, "inventory").build();
    }

    public EventSourcingController<Inventory, InventoryKey, InventoryEvent> getEventSourcingController() {
        return eventSourcingController;
    }

    private void initListeners(HazelcastInstance hazelcast) {
        String mapName = eventSourcingController.getCompletionMapName();
        IMap<PartitionedSequenceKey,CompletionInfo> completionsMap = hazelcast.getMap(mapName);
        completionsMap.addEntryListener(new EntryUpdatedListener<PartitionedSequenceKey, CompletionInfo>() {
            @Override
            public void entryUpdated(EntryEvent<PartitionedSequenceKey, CompletionInfo> entryEvent) {
                PartitionedSequenceKey key = entryEvent.getKey();
                CompletionInfo completion = entryEvent.getValue();
                System.out.println("entryUpdated listener triggered for " + key + " " + completion);

                EventCompletionHandler callback = awaitingCompletion.remove(key);
                if (callback != null) {
                    callback.eventProcessingComplete(key, null, completion);
                } else {
                    System.out.println("Missing completion observer for " + key);
                }
                completionsMap.remove(key);
            }

        }, true);
        System.out.println("Update listener armed on completions map");
    }

    private void initPipelines(HazelcastInstance hazelcast) {
        // none at this time
    }

    public boolean isEmbedded() { return embedded; }
    public byte[] getClientConfig() { return clientConfig; }
    public HazelcastInstance getHazelcastInstance() { return hazelcast; }

    public static void main(String[] args) throws IOException, InterruptedException {
        ServiceConfig.ServiceProperties props = ServiceConfig.get("inventory-service");
        InventoryService inventoryService = new InventoryService();
        inventoryService.initHazelcast(props.isEmbedded(), props.getClientConfig());

        inventoryService.initEventSourcingController(inventoryService.getHazelcastInstance());
        // Service must be initialized before pipelines, but after ESController.
        InventoryAPIImpl serviceImpl = new InventoryAPIImpl(inventoryService);
        inventoryService.initListeners(inventoryService.getHazelcastInstance());
        inventoryService.initPipelines(inventoryService.getHazelcastInstance());

        final GrpcServer server = new GrpcServer(serviceImpl, props.getGrpcPort());
        server.blockUntilShutdown();
    }
}
