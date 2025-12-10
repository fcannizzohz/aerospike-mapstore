package com.hazelcast.fcannizzohz.mapstoredemo;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class StringStringRecordMapStore extends AbstractAerospikeRecordMapStore<String, String> {

    @Override
    protected Key toAerospikeKey(String key) {
        return new Key(namespace, setName, key);
    }

    @Override
    protected String fromAerospikeKey(Key key) {
        if (key == null || key.userKey == null) {
            // Either skip in loadAllKeys, or derive from key.digest if you really want
            return null;
        }
        // Assumes userKey is a String
        return key.userKey.toString();
    }

    @Override
    protected Bin[] toBins(String key, String value) {
        return new Bin[] { new Bin("value", value) };
    }


    @Override
    protected String fromRecord(Key aerospikeKey, String logicalKey, Record record) {
        Object raw = record.getValue("value");
        return raw != null ? raw.toString() : null;
    }
}
