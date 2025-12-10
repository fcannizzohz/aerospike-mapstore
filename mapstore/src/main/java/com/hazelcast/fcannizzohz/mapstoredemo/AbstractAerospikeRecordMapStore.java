package com.hazelcast.fcannizzohz.mapstoredemo;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Abstract MapStore mapping a Hazelcast IMap<K,V> to Aerospike in a
 * 1-entry-per-record fashion:
 *
 *   - Aerospike namespace: configured via "aerospike.namespace" (default "test")
 *   - Aerospike set:       configured via "aerospike.set"        (default mapName)
 *   - Aerospike key:       derived from Hazelcast key via {@link #toAerospikeKey(Object)}
 *   - Aerospike bins:      derived from Hazelcast value via {@link #toBins(Object, Object)}
 *
 * Subclasses only need to implement the mapping hooks.
 */
public abstract class AbstractAerospikeRecordMapStore<K, V>
        implements MapStore<K, V>, MapLoaderLifecycleSupport, Closeable {

    protected AerospikeClient client;

    protected String namespace;
    protected String setName;

    // ---------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------

    @Override
    public void init(HazelcastInstance hazelcastInstance,
                     Properties properties,
                     String mapName) {

        this.namespace = properties.getProperty("aerospike.namespace", "test");
        this.setName   = properties.getProperty("aerospike.set", mapName);

        String host = properties.getProperty("aerospike.host", "127.0.0.1");
        int port    = Integer.parseInt(properties.getProperty("aerospike.port", "3000"));

        ClientPolicy clientPolicy = createClientPolicy(properties, mapName);
        this.client = new AerospikeClient(clientPolicy, host, port);

        afterInit(hazelcastInstance, properties, mapName);
    }

    /**
     * Customise Aerospike ClientPolicy if needed (timeouts, auth, etc.).
     */
    protected ClientPolicy createClientPolicy(Properties properties, String mapName) {
        return new ClientPolicy();
    }

    /**
     * Optional hook for extra initialisation after the client is created.
     */
    protected void afterInit(HazelcastInstance hazelcastInstance,
                             Properties properties,
                             String mapName) {
        // no-op by default
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
    // MapStore operations
    // ---------------------------------------------------------

    @Override
    public final V load(K key) {
        if (key == null) {
            return null;
        }

        try {
            Key aKey = toAerospikeKey(key);
            Record record = client.get(readPolicy(), aKey);
            if (record == null) {
                return null;
            }
            return fromRecord(aKey, key, record);
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
            Key[] aKeys = new Key[keyList.size()];
            for (int i = 0; i < keyList.size(); i++) {
                aKeys[i] = toAerospikeKey(keyList.get(i));
            }

            Record[] records = client.get(batchPolicy(), aKeys);
            Map<K, V> result = new HashMap<>(keyList.size());

            for (int i = 0; i < keyList.size(); i++) {
                Record record = records[i];
                if (record == null) {
                    continue;
                }
                K logicalKey = keyList.get(i);
                V value = fromRecord(aKeys[i], logicalKey, record);
                if (value != null) {
                    result.put(logicalKey, value);
                }
            }

            return result;
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException(
                    "Aerospike loadAll failed for " + keys.size() + " keys", e);
        }
    }

    /**
     * Full scan of the Aerospike set to enumerate keys.
     * Use only if you really need MapStore EAGER initial load.
     */
    @Override
    public Iterable<K> loadAllKeys() {
        try {
            ScanPolicy policy = scanPolicy();
            // Only keys needed
            policy.includeBinData = false;
            // **Important**: ask Aerospike to send userKey
            policy.sendKey = true;

            List<K> keys = new ArrayList<>();

            client.scanAll(policy, namespace, setName, (key, record) -> {
                K logicalKey = fromAerospikeKey(key);
                if (logicalKey != null) {
                    keys.add(logicalKey);
                }
            });
            return keys;
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException("Aerospike loadAllKeys (scan) failed", e);
        }
    }

    @Override
    public final void store(K key, V value) {
        if (key == null) {
            return;
        }

        try {
            Key aKey = toAerospikeKey(key);
            Bin[] bins = toBins(key, value);
            client.put(writePolicy(), aKey, bins);
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException("Aerospike store failed for key: " + key, e);
        }
    }

    @Override
    public final void storeAll(Map<K, V> map) {
        if (map == null || map.isEmpty()) {
            return;
        }

        // Simple implementation: per-entry put.
        // Override if you want custom batching/async behaviour.
        for (Map.Entry<K, V> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public final void delete(K key) {
        if (key == null) {
            return;
        }

        try {
            Key aKey = toAerospikeKey(key);
            client.delete(deletePolicy(), aKey);
        } catch (AerospikeException e) {
            throw new RetryableHazelcastException("Aerospike delete failed for key: " + key, e);
        }
    }

    @Override
    public final void deleteAll(Collection<K> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }

        for (K key : keys) {
            delete(key);
        }
    }

    // ---------------------------------------------------------
    // Policies – override in subclass if you want tuning
    // ---------------------------------------------------------

    /**
     * Read policy for single-key get(). Default: new Policy().
     */
    protected Policy readPolicy() {
        return new Policy();
    }

    /**
     * Batch policy for multi-key get(). Default: new BatchPolicy().
     */
    protected BatchPolicy batchPolicy() {
        return new BatchPolicy();
    }

    /**
     * Write policy for put() operations.
     */
    protected WritePolicy writePolicy() {
        WritePolicy wp = new WritePolicy();
        wp.sendKey = true;
        return wp;
    }

    /**
     * Write policy for delete() operations.
     * Default: same as writePolicy().
     */
    protected WritePolicy deletePolicy() {
        return writePolicy();
    }

    /**
     * Scan policy for loadAllKeys() scans.
     */
    protected ScanPolicy scanPolicy() {
        ScanPolicy sp = new ScanPolicy();
        sp.sendKey = true;
        return sp;
    }

    // ---------------------------------------------------------
    // Mapping hooks – subclasses must implement
    // ---------------------------------------------------------

    /**
     * Map Hazelcast key to Aerospike Key.
     * Example:
     *   return new Key(namespace, setName, key);
     */
    protected abstract Key toAerospikeKey(K key);

    /**
     * Map Aerospike Key back to Hazelcast key (used in loadAllKeys / scan).
     * Example:
     *   return (K) key.userKey.toString();
     */
    protected abstract K fromAerospikeKey(Key key);

    /**
     * Map Hazelcast value to bins for a given key.
     * Example:
     *   return new Bin[] {
     *       new Bin("field1", value.getField1()),
     *       new Bin("field2", value.getField2())
     *   };
     */
    protected abstract Bin[] toBins(K key, V value);

    /**
     * Map an Aerospike Record back to Hazelcast value.
     * Example:
     *   String f1 = record.getString("field1");
     *   int f2    = record.getInt("field2");
     *   return new MyValue(f1, f2);
     */
    protected abstract V fromRecord(Key aerospikeKey, K logicalKey, Record record);
}
