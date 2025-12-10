package com.hazelcast.fcannizzohz.mapstoredemo;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeTestClient {
    // ---- Start Aerospike client against Testcontainers instance ----
    public static final String AS_HOST = "localhost";
    public static final int AS_PORT = 3000;
    private final AerospikeClient aerospikeClient;
    public final String namespace;
    public final String setName;
    public final String mapBinName;
    public final String recordKeyString;
    public final String valueBin;

    public AerospikeTestClient() {
        ClientPolicy clientPolicy = new ClientPolicy();
        aerospikeClient = new AerospikeClient(clientPolicy, AS_HOST, AS_PORT);

        boolean alive = aerospikeClient.isConnected();
        if (!alive) {
            throw new RuntimeException("AerospikeClient is not connected.");
        }
        this.namespace = "test";
        this.setName = "samples_set";
        this.mapBinName = "samples_bin";
        this.recordKeyString =  "samples-record";
        this.valueBin = "value";
    }

    public void prepopulateCdtAerospike() {
        // Build the record key and CDT map with 5 entries
        Key key = new Key(namespace, setName, recordKeyString);

        Map<Value, Value> items = new HashMap<>();
        items.put(Value.get("k1"), Value.get("v1"));
        items.put(Value.get("k2"), Value.get("v2"));
        items.put(Value.get("k3"), Value.get("v3"));
        items.put(Value.get("k4"), Value.get("v4"));
        items.put(Value.get("k5"), Value.get("v5"));

        MapPolicy mapPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
        WritePolicy writePolicy = new WritePolicy();

        aerospikeClient.operate(
                writePolicy,
                key,
                MapOperation.putItems(mapPolicy, mapBinName, items)
        );
    }

    public void close() {
        if(aerospikeClient.isConnected()) {
            aerospikeClient.close();
        }
    }

    public Object get(String k) {
        Key key = new Key(namespace, setName, recordKeyString);

        WritePolicy readPolicy = new WritePolicy(); // operate uses WritePolicy
        Record record = aerospikeClient.operate(
                readPolicy,
                key,
                MapOperation.getByKey(mapBinName, Value.get(k), MapReturnType.VALUE)
        );

        return record.getValue(mapBinName);
    }

    public Object getRecord(String k) {
        Policy readPolicy = new Policy();
        readPolicy.sendKey = true;
        Key key = new Key(namespace, setName, k);
        Record record = aerospikeClient.get(readPolicy, key);
        return record != null ? record.getValue(valueBin) : null;
    }

    public void prepopulateRecordAerospike() {
        WritePolicy wp = new WritePolicy();

        // Clean any old data for k1..k6
        for (String keyStr : new String[]{"k1", "k2", "k3", "k4", "k5", "k6"}) {
            Key key = new Key(namespace, setName, keyStr);
            aerospikeClient.delete(wp, key);
        }

        putRecord("k1", "v1");
        putRecord("k2", "v2");
        putRecord("k3", "v3");
        putRecord("k4", "v4");
        putRecord("k5", "v5");
    }

    private void putRecord(String keyStr, String valueStr) {
        WritePolicy wp = new WritePolicy();
        Key key = new Key(namespace, setName, keyStr);
        Bin bin = new Bin(valueBin, valueStr);
        aerospikeClient.put(wp, key, bin);
    }

}
