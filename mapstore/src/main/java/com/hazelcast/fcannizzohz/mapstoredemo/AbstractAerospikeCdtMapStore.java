package com.hazelcast.fcannizzohz.mapstoredemo;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.cdt.MapReturnType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Abstract MapStore that maps a Hazelcast IMap<K,V> into a single
 * Aerospike record containing a CDT Map bin.
 * <p/>
 * - One Aerospike record per Hazelcast map.
 * - Each IMap entry (K,V) is a (mapKey → mapValue) entry in that bin.
 * <p/>
 * All metadata (namespace, set, record primary key, bin name, etc.)
 * comes from MapStore properties.
 */
public abstract class AbstractAerospikeCdtMapStore<K, V>
        implements MapStore<K, V>, MapLoaderLifecycleSupport, Closeable {

    protected AerospikeClient client;

    protected String namespace;
    protected String setName;
    protected String mapBinName;

    /**
     * Primary key of the record that holds the CDT map.
     * Same for all entries of this Hazelcast map.
     */
    protected Key recordKey;

    // ---------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------

    @Override
    public void init(HazelcastInstance hazelcastInstance,
                     Properties properties,
                     String mapName) {

        this.namespace  = properties.getProperty("aerospike.namespace", "test");
        this.setName    = properties.getProperty("aerospike.set", mapName);
        this.mapBinName = properties.getProperty("aerospike.mapBinName", "mapbin");

        String host = properties.getProperty("aerospike.host", "127.0.0.1");
        int port    = Integer.parseInt(properties.getProperty("aerospike.port", "3000"));

        ClientPolicy clientPolicy = createClientPolicy(properties, mapName);
        this.client = new AerospikeClient(clientPolicy, host, port);

        this.recordKey = createRecordKey(properties, mapName);

        afterInit(hazelcastInstance, properties, mapName);
    }

    protected ClientPolicy createClientPolicy(Properties properties, String mapName) {
        return new ClientPolicy();
    }

    /**
     * Build the Aerospike primary key for the CDT map record from properties.
     * <p/>
     * Properties:
     *  - aerospike.recordKeyType = string|int|long (default: string)
     *  - aerospike.recordKey     = value (default: mapName)
     */
    protected Key createRecordKey(Properties properties, String mapName) {
        String keyType  = properties.getProperty("aerospike.recordKeyType", "string")
                                    .toLowerCase(Locale.ROOT);
        String rawValue = properties.getProperty("aerospike.recordKey", mapName);

        return switch (keyType) {
            case "int"   -> new Key(namespace, setName, Integer.parseInt(rawValue));
            case "long"  -> new Key(namespace, setName, Long.parseLong(rawValue));
            case "string" -> new Key(namespace, setName, rawValue);
            default      -> throw new IllegalArgumentException("Unsupported recordKeyType: " + keyType);
        };
    }

    /**
     * Hook for subclasses if they need extra init based on Hazelcast or properties.
     */
    protected void afterInit(HazelcastInstance hazelcastInstance,
                             Properties properties,
                             String mapName) {
        // no-op
    }

    @Override
    public void destroy() {
        try {
            close();
        } catch (IOException ignored) {
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null && client.isConnected()) {
            client.close();
        }
    }

    // ---------------------------------------------------------
    // MapStore operations – use CDT Map under the hood
    // ---------------------------------------------------------

    @Override
    public final V load(K key) {
        if (key == null) {
            return null;
        }

        try {
            Value mapKey = toAerospikeMapKey(key);

            Record record = client.operate(
                    readOperatePolicy(),
                    recordKey,
                    MapOperation.getByKey(mapBinName, mapKey, MapReturnType.VALUE)
            );

            if (record == null) {
                return null;
            }

            Object raw = record.getValue(mapBinName);
            if (raw == null) {
                return null;
            }

            return fromAerospikeMapValue(key, Value.get(raw));
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException("Aerospike load failed for key: " + key, e);
        }
    }

    @Override
    public final Map<K, V> loadAll(Collection<K> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            List<K> keyList = new ArrayList<>(keys);
            List<Value> mapKeys = new ArrayList<>(keyList.size());
            for (K k : keyList) {
                mapKeys.add(toAerospikeMapKey(k));
            }

            Record record = client.operate(
                    readOperatePolicy(),
                    recordKey,
                    MapOperation.getByKeyList(mapBinName, mapKeys, MapReturnType.VALUE)
            );

            Map<K, V> result = new HashMap<>(keyList.size());

            if (record == null) {
                return result;
            }

            Object raw = record.getValue(mapBinName);
            if (raw == null) {
                return result;
            }

            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) raw;

            for (int i = 0; i < keyList.size(); i++) {
                Object elem = values.get(i);
                if (elem == null) {
                    continue;
                }
                V value = fromAerospikeMapValue(keyList.get(i), Value.get(elem));
                if (value != null) {
                    result.put(keyList.get(i), value);
                }
            }

            return result;
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException(
                    "Aerospike loadAll failed for " + keys.size() + " keys", e);
        }
    }

    @Override
    public Iterable<K> loadAllKeys() {
        // You can’t avoid “full scan” here anyway because all entries live in one record, so loadAllKeys() just means “read that record and return its map keys”.
        try {
            // Plain get on the CDT map record
            com.aerospike.client.policy.Policy policy = new com.aerospike.client.policy.Policy();
            Record record = client.get(policy, recordKey);

            if (record == null) {
                return java.util.Collections.emptyList();
            }

            Object raw = record.getValue(mapBinName);
            if (raw == null) {
                return java.util.Collections.emptyList();
            }

            return getKeys(raw);
        } catch (com.aerospike.client.AerospikeException e) {
            throw new com.hazelcast.spi.exception.RetryableHazelcastException(
                    "Aerospike loadAllKeys failed", e);
        }
    }

    private List<K> getKeys(Object raw) {
        if (!(raw instanceof Map<?, ?> rawMap)) {
            throw new IllegalStateException(
                    "Expected CDT map bin '" + mapBinName + "' to be a Map, but was: "
                            + raw.getClass().getName()
            );
        }

        List<K> keys = new ArrayList<>(rawMap.size());
        for (Object keyObj : rawMap.keySet()) {
            @SuppressWarnings("unchecked")
            K key = (K) keyObj; // assumes Aerospike map key type is compatible with K
            keys.add(key);
        }
        return keys;
    }

    @Override
    public final void store(K key, V value) {
        if (key == null) {
            return;
        }

        try {
            Value mapKey   = toAerospikeMapKey(key);
            Value mapValue = toAerospikeMapValue(value);

            client.operate(
                    writePolicy(),
                    recordKey,
                    MapOperation.put(mapPolicy(), mapBinName, mapKey, mapValue)
            );
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException("Aerospike store failed for key: " + key, e);
        }
    }

    @Override
    public final void storeAll(Map<K, V> map) {
        if (map == null || map.isEmpty()) {
            return;
        }

        try {
            Map<Value, Value> items = new HashMap<>(map.size());
            for (Map.Entry<K, V> entry : map.entrySet()) {
                items.put(
                        toAerospikeMapKey(entry.getKey()),
                        toAerospikeMapValue(entry.getValue())
                );
            }

            client.operate(
                    writePolicy(),
                    recordKey,
                    MapOperation.putItems(mapPolicy(), mapBinName, items)
            );
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException(
                    "Aerospike storeAll failed for " + map.size() + " entries", e);
        }
    }

    @Override
    public final void delete(K key) {
        if (key == null) {
            return;
        }

        try {
            Value mapKey = toAerospikeMapKey(key);
            client.operate(
                    deletePolicy(),
                    recordKey,
                    MapOperation.removeByKey(mapBinName, mapKey, MapReturnType.NONE)
            );
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException("Aerospike delete failed for key: " + key, e);
        }
    }

    @Override
    public final void deleteAll(Collection<K> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }

        try {
            List<Value> mapKeys = new ArrayList<>(keys.size());
            for (K k : keys) {
                mapKeys.add(toAerospikeMapKey(k));
            }

            client.operate(
                    deletePolicy(),
                    recordKey,
                    MapOperation.removeByKeyList(mapBinName, mapKeys, MapReturnType.NONE)
            );
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException(
                    "Aerospike deleteAll failed for " + keys.size() + " keys", e);
        }
    }

    // ---------------------------------------------------------
    // Policies – override in subclass if you want tuning
    // ---------------------------------------------------------

    /**
     * Policy used for "read" operates (CDT getByKey / getByKeyList).
     */
    protected WritePolicy readOperatePolicy() {
        // In Aerospike Java client, operate always uses WritePolicy
        WritePolicy wp = new WritePolicy();
        // You can set wp.readModeAP / readModeSC / socketTimeout, etc. here
        return wp;
    }

    protected WritePolicy writePolicy() {
        return new WritePolicy();
    }

    /**
     * Separate hook in case you want different timeout / commit semantics for deletes.
     * Default: same as writePolicy().
     */
    protected WritePolicy deletePolicy() {
        return writePolicy();
    }

    protected MapPolicy mapPolicy() {
        // Default: ordered by key, default flags
        return new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
    }

    // ---------------------------------------------------------
    // Mapping hooks – subclasses define how K/V ↔ Value
    // ---------------------------------------------------------

    /**
     * Convert Hazelcast key into Aerospike map key (CDT key).
     */
    protected abstract Value toAerospikeMapKey(K key);

    /**
     * Convert Hazelcast value into Aerospike map value (CDT value).
     */
    protected abstract Value toAerospikeMapValue(V value);

    /**
     * Convert Aerospike map value back into Hazelcast value.
     */
    protected abstract V fromAerospikeMapValue(K key, Value value);
}
