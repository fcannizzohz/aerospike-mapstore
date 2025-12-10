package com.hazelcast.fcannizzohz.mapstoredemo;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.map.IMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.fcannizzohz.mapstoredemo.AerospikeTestClient.AS_HOST;
import static com.hazelcast.fcannizzohz.mapstoredemo.AerospikeTestClient.AS_PORT;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.assertj.core.api.Assertions.assertThat;

public class StringStringRecordMapStoreITest {

    private static TestHazelcastFactory hazelcastFactory;
    private static AerospikeTestClient aerospikeClient;

    private HazelcastInstance hazelcast;

    @BeforeClass
    public static void setUpClass() {
        // Connect to Aerospike
        aerospikeClient = new AerospikeTestClient();// ---- Pre-populate CDT map in Aerospike ----
        aerospikeClient.prepopulateRecordAerospike();

        // Start Hazelcast factory
        hazelcastFactory = new TestHazelcastFactory();
    }

    @AfterClass
    public static void tearDownClass() {
        if (hazelcastFactory != null) {
            hazelcastFactory.terminateAll();
        }
        if (aerospikeClient != null) {
            aerospikeClient.close();
        }
    }

    @Before
    public void setUp() {
        hazelcast = hazelcastFactory.newHazelcastInstance(newHzConfig());
    }

    @After
    public void tearDown() {
        if (hazelcastFactory != null) {
            hazelcastFactory.shutdownAll();
        }
    }


    private Config newHzConfig() {
        Config config = new Config()
                .setJetConfig(new JetConfig().setEnabled(true))
                .setClusterName(randomName())
                .setLicenseKey(System.getenv("HZ_LICENSEKEY"));

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(new StringStringRecordMapStore())
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);

        Properties props = new Properties();
        props.setProperty("aerospike.host", AS_HOST);
        props.setProperty("aerospike.port", String.valueOf(AS_PORT));
        props.setProperty("aerospike.namespace", aerospikeClient.namespace);
        props.setProperty("aerospike.set", aerospikeClient.setName);
        // If your StringStringAerospikeRecordMapStore reads a bin name from properties,
        // uncomment and align the key:
        // props.setProperty("aerospike.valueBinName", VALUE_BIN);

        mapStoreConfig.setProperties(props);

        config.getMapConfig("samples")
              .setMapStoreConfig(mapStoreConfig);

        return config;
    }

    @Test
    public void testLazyLoadFromAerospike() {
        IMap<String, String> map = hazelcast.getMap("samples");

        // Trigger lazy load via getAll
        Set<String> keys = Set.of("k1", "k2", "k3", "k4", "k5");
        Map<String, String> loaded = map.getAll(keys);

        assertThat(loaded).hasSize(5);
        assertThat(loaded.get("k1")).isEqualTo("v1");
        assertThat(loaded.get("k5")).isEqualTo("v5");
    }

    @Test
    public void testStoreWritesBackToAerospike() {
        IMap<String, String> map = hazelcast.getMap("samples");

        // Put a new entry via Hazelcast
        map.put("k6", "v6");

        // Verify directly in Aerospike
        Object raw = aerospikeClient.getRecord("k6");
        assertThat(raw).isNotNull();
        assertThat(raw.toString()).isEqualTo("v6");
    }
}
