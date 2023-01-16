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

package org.hazelcast.msfdemo.invsvc.business;


import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.hazelcast.eventsourcing.EventSourcingController;
import org.hazelcast.eventsourcing.event.PartitionedSequenceKey;
import org.hazelcast.eventsourcing.sync.CompletionInfo;
import org.hazelcast.eventsourcing.sync.EventCompletionHandler;
import org.hazelcast.msfdemo.invsvc.domain.Inventory;
import org.hazelcast.msfdemo.invsvc.domain.InventoryKey;
import org.hazelcast.msfdemo.invsvc.domain.Item;
import org.hazelcast.msfdemo.invsvc.events.InventoryEvent;
import org.hazelcast.msfdemo.invsvc.events.InventoryGrpc;
import org.hazelcast.msfdemo.invsvc.events.PullInventoryEvent;
import org.hazelcast.msfdemo.invsvc.events.ReserveInventoryEvent;
import org.hazelcast.msfdemo.invsvc.service.InventoryService;
import org.hazelcast.msfdemo.invsvc.views.InventoryDAO;
import org.hazelcast.msfdemo.invsvc.views.ItemDAO;

import java.util.logging.Logger;

import static org.hazelcast.msfdemo.invsvc.events.InventoryOuterClass.*;

public class InventoryAPIImpl extends InventoryGrpc.InventoryImplBase {

    private static final Logger logger = Logger.getLogger(InventoryAPIImpl.class.getName());
    InventoryDAO inventoryDAO;
    ItemDAO itemDAO;

    private InventoryService inventoryService;
    private EventSourcingController<Inventory,InventoryKey, InventoryEvent> eventSourcingController;
    //private final IMap<String, APIBufferPair> bufferPairsForAPI;
//    APIBufferPair<OpenAccountRequest,OpenAccountResponse> openHandler;
//    APIBufferPair<AdjustBalanceRequest,AdjustBalanceResponse> balanceAdjustHandler;


    private int unacknowledgedAddInventoryRequests = 0;
    private int airBatchSize = 1000;

    public InventoryAPIImpl(InventoryService service) {
        this.inventoryService = service;
        this.eventSourcingController = service.getEventSourcingController();
        String serviceName = bindService().getServiceDescriptor().getName();
        logger.info("AccountAPIImpl initializing structures for " + serviceName);
        // If we're not implementing pipelines we don't need buffer pairs ...
        //bufferPairsForAPI = hazelcast.getMap(serviceName+"_APIS");
        // TODO: init buffer pairs for each API
        inventoryDAO = new InventoryDAO(service.getHazelcastInstance());
        itemDAO = new ItemDAO(service.getHazelcastInstance());
    }

    @Override
    public void clearAllData(ClearAllDataRequest request, StreamObserver<ClearAllDataResponse> response) {
        Context ctx = Context.current().fork();
        ctx.run(() -> {
            inventoryDAO.deleteAll();
            itemDAO.deleteAll();
        });
        response.onNext(ClearAllDataResponse.newBuilder().build());
        response.onCompleted();
        logger.info("*** All Item & Inventory Data cleared ***");
    }

    @Override
    public StreamObserver<AddItemRequest> addItem(StreamObserver<AddItemResponse> response) {
        return new StreamObserver<AddItemRequest>() {
            @Override
            public void onNext(AddItemRequest addItemRequest) {
                Item item = new Item();
                item.setItemNumber(addItemRequest.getItemNumber());
                item.setDescription(addItemRequest.getDescription());
                item.setCategoryID(addItemRequest.getCategoryID());
                item.setCategoryName(addItemRequest.getCategoryName());
                item.setPrice(addItemRequest.getPrice());
                itemDAO.insert(item.getItemNumber(), item);
                //System.out.println("InventoryAPIItem.addItem inserted " + item.getItemNumber());
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("InvAPI AddItemRequest.onError:");
                throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
                // Empty response
                response.onNext(AddItemResponse.newBuilder().build());
                response.onCompleted();
                System.out.println("InventoryAPIImpl.addItem.onCompleted: all items done");
            }
        };
    }

    @Override
    public StreamObserver<AddInventoryRequest> addInventory(StreamObserver<AddInventoryResponse> response) {
        //Context ctx = Context.current().fork(); // attempt to avoid 'client cancelled' errors
        return new StreamObserver<>() {
            @Override
            public void onNext(AddInventoryRequest addInventoryRequest) {
                //ctx.run(() -> {
                unacknowledgedAddInventoryRequests++;
                Inventory stock = new Inventory();
                stock.setItemNumber(addInventoryRequest.getItemNumber());
                stock.setDescription(addInventoryRequest.getDescription());
                stock.setLocation(addInventoryRequest.getLocation());
                stock.setLocationType(addInventoryRequest.getLocationType());
                stock.setGeohash(addInventoryRequest.getGeohash());
                stock.setQuantityOnHand(addInventoryRequest.getQtyOnHand());
                stock.setQuantityReserved(addInventoryRequest.getQtyReserved());
                stock.setAvailableToPromise(addInventoryRequest.getAvailToPromise());
                InventoryKey key = new InventoryKey(stock.getItemNumber(), stock.getLocation());
                inventoryDAO.insert(key, stock);
                if (unacknowledgedAddInventoryRequests > airBatchSize) {
                    AddInventoryResponse batchAck = AddInventoryResponse.newBuilder().setAckCount(airBatchSize).build();
                    response.onNext(batchAck);
                    unacknowledgedAddInventoryRequests -= airBatchSize;
                    //System.out.println("Acknowledged " + airBatchSize + " AddInventory requests, " + unacknowledgedAddInventoryRequests + "  still in flight");
                }
                //});
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("InvAPI AddInventoryRequest.onError:");
                throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
                // Empty response
                response.onNext(AddInventoryResponse.newBuilder().build());
                response.onCompleted();
                System.out.println("InventoryAPIImpl.addInventory complete");
            }
        };
    }

    @Override
    public void reserve(ReserveRequest request, StreamObserver<ReserveResponse> responseObserver) {
        String itemNumber = request.getItemNumber();
        String location = request.getLocation();
        int quantity = request.getQuantity();
        int duration = request.getDurationMinutes(); // NOT CURRENTLY USING
        System.out.println("Reserve request " + itemNumber + " " + location + " " + quantity);

        // Get ATP from DAO, if not available we fail fast
        InventoryKey invKey = new InventoryKey(itemNumber, location);
        Inventory inv = inventoryDAO.findByKey(invKey);
        if (inv == null) {
            ReserveResponse nomatch = ReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setReason("No record exists for item/location combination")
                    .build();
            responseObserver.onNext(nomatch);
            responseObserver.onCompleted();
            return;
        }

        if (inv.getAvailableToPromise() < request.getQuantity()) {
            System.out.printf("Insufficient ATP %d %d %d\n", inv.getQuantityOnHand(), inv.getQuantityReserved(), inv.getAvailableToPromise());
            ReserveResponse shortage = ReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setReason(("Insufficient quantity available"))
                    .build();
            responseObserver.onNext(shortage);
            responseObserver.onCompleted();
            return;
        }

        System.out.println("Reserve success, updating event store and view");

        // Create Event object
        ReserveInventoryEvent event = new ReserveInventoryEvent(invKey, request.getQuantity());
        System.out.println("ReserveInventoryEvent has key " + event.getKey());
        PartitionedSequenceKey eventKey = null;

        try {
            eventKey = eventSourcingController.handleEvent(event);
        } catch (Exception e) {
            e.printStackTrace();
//            ReserveResponse fail = ReserveResponse.newBuilder()
//                    .setSuccess(false)
//                    .setReason(e.getLocalizedMessage())
//                    .build();
            responseObserver.onError(e);
            responseObserver.onCompleted();
            return;
        }

        // TODO: potential race condition here .. pipeline might complete before we get our observer
        //  registered.
        inventoryService.registerObserver(eventKey, (partitionedSequenceKey, inventoryEvent, completionInfo) -> {
            if (completionInfo.status == CompletionInfo.Status.COMPLETED_OK) {
                // Respond to caller
                ReserveResponse success = ReserveResponse.newBuilder()
                        .setSuccess(true).build();
                responseObserver.onNext(success);
                responseObserver.onCompleted();
            } else if (completionInfo.status == CompletionInfo.Status.HAD_ERROR) {
//                ReserveResponse fail = ReserveResponse.newBuilder()
//                        .setSuccess(false)
//                        .setReason((completionInfo.error.getLocalizedMessage()))
//                        .build();
                responseObserver.onError(completionInfo.error);
                responseObserver.onCompleted();
            }
            // TODO: not handling timed out because feature not implemented yet
        });
    }

    // TODO: completion handling not implemented, waiting for it to fully test out in reserve
    @Override
    public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
        String itemNumber = request.getItemNumber();
        String location = request.getLocation();
        int quantity = request.getQuantity();
        System.out.println("Pull request " + itemNumber + " " + location + " " + quantity);

        // Get ATP from DAO, if not available we fail fast
        InventoryKey invKey = new InventoryKey(itemNumber, location);
        Inventory inv = inventoryDAO.findByKey(invKey);
        if (inv == null) {
            PullResponse nomatch = PullResponse.newBuilder()
                    .setSuccess(false)
                    .setReason("No record exists for item/location combination")
                    .build();
            responseObserver.onNext(nomatch);
            responseObserver.onCompleted();
            return;
        }

        if (inv.getAvailableToPromise() + inv.getQuantityReserved() < request.getQuantity() ) {
            System.out.printf("Insufficient ATP %d %d %d\n", inv.getQuantityOnHand(), inv.getQuantityReserved(), inv.getAvailableToPromise());
            PullResponse shortage = PullResponse.newBuilder()
                    .setSuccess(false)
                    .setReason(("Insufficient quantity available"))
                    .build();
            responseObserver.onNext(shortage);
            responseObserver.onCompleted();
            return;
        }

        System.out.println("Pull success, updating event store and view");

        // Create Event object
        PullInventoryEvent event = new PullInventoryEvent(invKey, request.getQuantity());
        PartitionedSequenceKey eventKey = null;

        try {
            eventKey = eventSourcingController.handleEvent(event);
        } catch (Exception e) {
            e.printStackTrace();
            responseObserver.onError(e);
            responseObserver.onCompleted();
            return;
        }

        inventoryService.registerObserver(eventKey, (partitionedSequenceKey, inventoryEvent, completionInfo) -> {
            if (completionInfo.status == CompletionInfo.Status.COMPLETED_OK) {
                // Respond to caller
                PullResponse success = PullResponse.newBuilder()
                        .setSuccess(true).build();
                responseObserver.onNext(success);
                responseObserver.onCompleted();
            } else if (completionInfo.status == CompletionInfo.Status.HAD_ERROR) {
                responseObserver.onError(completionInfo.error);
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void unreserve(ReserveRequest request, StreamObserver<ReserveResponse> responseObserver) {
        System.out.println("unreserve unimplemented in InventoryAPIImpl");
    }

    @Override
    public void restock(PullRequest request, StreamObserver<PullResponse> responseObserver) {
        System.out.println("restock unimplemented in InventoryAPIImpl");
    }

    @Override
    public void getItemCount(ItemCountRequest request, StreamObserver<ItemCountResponse> responseObserver) {
        // Request is empty so ignore it
        ItemCountResponse response = ItemCountResponse.newBuilder()
            .setCount(itemDAO.getItemCount())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getInventoryRecordCount(InventoryCountRequest request, StreamObserver<InventoryCountResponse> responseObserver) {
        // Request is empty so ignore it
        InventoryCountResponse response = InventoryCountResponse.newBuilder()
                .setCount(inventoryDAO.getInventoryRecordCount())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
