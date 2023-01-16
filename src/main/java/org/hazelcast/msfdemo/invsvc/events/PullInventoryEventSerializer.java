package org.hazelcast.msfdemo.invsvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.msfdemo.invsvc.domain.InventoryKey;

import javax.annotation.Nonnull;

public class PullInventoryEventSerializer implements CompactSerializer<PullInventoryEvent> {
    @Nonnull
    @Override
    public PullInventoryEvent read(@Nonnull CompactReader compactReader) {
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
        PullInventoryEvent event = new PullInventoryEvent(key, quantity);
        return event;    }

    @Override
    public void write(@Nonnull CompactWriter compactWriter, @Nonnull PullInventoryEvent pullInventoryEvent) {
        compactWriter.writeString("itemNumber", pullInventoryEvent.getKey().itemNumber);
        compactWriter.writeString("location", pullInventoryEvent.getKey().locationID);
        compactWriter.writeString("eventClass", pullInventoryEvent.getEventClass());
        compactWriter.writeInt64("timestamp", pullInventoryEvent.getTimestamp());
        compactWriter.writeCompact("payload", pullInventoryEvent.getPayload());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return PullInventoryEvent.class.getCanonicalName();
    }

    @Nonnull
    @Override
    public Class<PullInventoryEvent> getCompactClass() {
        return PullInventoryEvent.class;
    }
}
