package org.hazelcast.msfdemo.invsvc.domain;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.eventsourcing.event.HydrationFactory;
import org.hazelcast.msfdemo.invsvc.events.InventoryCompactionEvent;
import org.hazelcast.msfdemo.invsvc.events.InventoryEvent;
import org.hazelcast.msfdemo.invsvc.events.PullInventoryEvent;
import org.hazelcast.msfdemo.invsvc.events.ReserveInventoryEvent;

import java.io.Serializable;

public class InventoryHydrationFactory
        implements HydrationFactory<Inventory, InventoryKey, InventoryEvent>, Serializable {

    // Although there are two DOs in this module (Item and Inventory), the only Events we currently
    // support are those that modify state of the Inventory object; Items are not modified after
    // creation.
    public static final String DO_NAME = "InventoryService.inventory";
    private String mapping_template = "CREATE MAPPING IF NOT EXISTS \"?\" (\n" +
            // SourcedEvent fields
            // Key is PartitionedSequenceKey here -- will be cast as VARCHAR
            //  in query which may break if domain object key isn't a String
            //  External names are as declared in PartitionedSequenceKey.java
            "   doKey        OBJECT    EXTERNAL NAME \"__key.domainObjectKey\",\n" +
            "   sequence     BIGINT    EXTERNAL NAME \"__key.sequence\",\n" +
            "   eventName    VARCHAR,\n" +
            // InventoryEvent fields
            "   itemNumber         VARCHAR,\n" +
            "   description        VARCHAR,\n" +
            "   location           VARCHAR,\n" +
            "   locationType       VARCHAR,\n" +
            "   geohash            VARCHAR,\n" +
            "   quantity           INTEGER\n"  +
    // These fields are on the domain object, not on the event
//            "   quantityOnHand     INTEGER,\n" +
//            "   quantityReserved   INTEGER,\n" +
//            "   availToPromise     INTEGER\n"  +
            ")\n" +
            "TYPE IMap\n" +
            "OPTIONS (\n" +
            "  'keyFormat' = 'java',\n" +
            "  'keyJavaClass' = 'org.hazelcast.eventsourcing.event.PartitionedSequenceKey',\n" +
            "  'valueFormat' = 'compact',\n" +
            "  'valueCompactTypeName' = 'InventoryService.InventoryEvent'\n" +
            ")";    @Override

    // TODO: Add doName field, return type then becomes DomainObject, presumably
    public Inventory hydrateDomainObject(GenericRecord genericRecord) {
                System.out.println("hydrateDomainObject with GR : " + genericRecord);
                return new Inventory(genericRecord);
    }

    @Override
    public InventoryEvent hydrateEvent(String eventName, SqlRow data) {
        switch (eventName) {
            case "InventoryService.ReserveInventoryEvent":
                return new ReserveInventoryEvent(data);
            case "InventoryService.PullInventoryEvent":
                return new PullInventoryEvent(data);
            case "InventoryService.InventoryCompactionEvent":
                //return new InventoryCompactionEvent(data);
            default:
                throw new IllegalArgumentException("bad eventName: " + eventName);
        }
    }

    @Override
    public InventoryEvent hydrateEvent(String eventName, GenericRecord data) {
        switch (eventName) {
            case "InventoryService.ReserveInventoryEvent":
                return new ReserveInventoryEvent(data);
            case "InventoryService.PullInventoryEvent":
                return new PullInventoryEvent(data);
            case "InventoryService.InventoryCompactionEvent":
                return new InventoryCompactionEvent(data);
            default:
                throw new IllegalArgumentException("bad eventName: " + eventName);
        }
    }

    @Override
    public String getEventMapping(String eventStoreName) {
        mapping_template = mapping_template.replaceAll("\\?", eventStoreName);
        return mapping_template;
    }
}
