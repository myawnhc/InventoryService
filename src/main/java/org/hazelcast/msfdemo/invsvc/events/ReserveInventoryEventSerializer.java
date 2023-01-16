package org.hazelcast.msfdemo.invsvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.msfdemo.invsvc.domain.InventoryKey;

import javax.annotation.Nonnull;

public class ReserveInventoryEventSerializer implements CompactSerializer<ReserveInventoryEvent> {
    @Nonnull
    @Override
    public ReserveInventoryEvent read(@Nonnull CompactReader compactReader) {
        String itemNumber = compactReader.readString("itemNumber");
        String location = compactReader.readString("location");
        String eventClass = compactReader.readString("eventClass");
        long timestamp = compactReader.readInt64("timestamp");
        HazelcastJsonValue payload = compactReader.readCompact("payload");
        JSONObject jobj = new JSONObject(payload.getValue());
//        String itemNumber = jobj.getString("itemNumber");
//        String location = jobj.getString("location");
        int quantity = jobj.getInt("quantity");
        InventoryKey key = new InventoryKey(itemNumber, location);
        ReserveInventoryEvent event = new ReserveInventoryEvent(key, quantity);
        return event;
    }

    @Override
    public void write(@Nonnull CompactWriter compactWriter, @Nonnull ReserveInventoryEvent reserveInventoryEvent) {
        //compactWriter.writeString("key", reserveInventoryEvent.getKey());
        compactWriter.writeString("itemNumber", reserveInventoryEvent.getKey().itemNumber);
        compactWriter.writeString("location", reserveInventoryEvent.getKey().locationID);
        compactWriter.writeString("eventClass", reserveInventoryEvent.getEventClass());
        compactWriter.writeInt64("timestamp", reserveInventoryEvent.getTimestamp());
        compactWriter.writeCompact("payload", reserveInventoryEvent.getPayload());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return ReserveInventoryEvent.class.getCanonicalName();
    }

    @Nonnull
    @Override
    public Class<ReserveInventoryEvent> getCompactClass() {
        return ReserveInventoryEvent.class;
    }
}
