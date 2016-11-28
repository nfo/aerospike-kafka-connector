package com.aerospike.kafka.connect.sink;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.*;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.kafka.connect.data.AerospikeRecord;

public class SyncWriterTest {

    static final ConnectorConfig config = defaultConfig();
    static AerospikeClient syncClient;

    @BeforeClass
    public static void beforeClass() {
        syncClient = new AerospikeClient(null, config.getHosts());
    }
    
    @AfterClass
    public static void afterClass() {
        syncClient.close();
    }
    
    @Test
    public void testWriteAndFlush() {
        SyncWriter writer = new SyncWriter(config);
        Random rnd = new Random();
        int noRecords = 100;
        Key[] keys = new Key[noRecords];
        AerospikeRecord[] records = new AerospikeRecord[noRecords];
        for (int i = 0; i < noRecords; i++) {
            int id = 100_000 + rnd.nextInt(900_000);
            Key key = new Key("test", "test", "SyncWriterTest" + rnd.nextInt());
            Bin[] bins = new Bin[] { new Bin("id", id), new Bin("str", "aString"), new Bin("int", 1234) };
            keys[i] = key;
            records[i] = new AerospikeRecord(key, bins);
        }

        for (AerospikeRecord r : records) {
            writer.write(r);
        }
        writer.flush();

        assertRecordsExist(keys);
    }
    
    private void assertRecordsExist(Key[] keys) {
        Record[] records = syncClient.get(null, keys);
        for (Record r : records) {
            assertNotNull("One or more records are missing", r);
        }
    }

    private static ConnectorConfig defaultConfig() {
        String hosts = System.getenv("AEROSPIKE_HOSTS");
        if (hosts == null || hosts.isEmpty()) {
            hosts = "127.0.0.1";
        }
        Map<String, String> props = new HashMap<>();
        props.put("cluster.hosts", hosts);
        props.put("topics", "testTopic");
        props.put("topic.namespace", "test");
        props.put("topic.set", "test");
        return new ConnectorConfig(props);
    }
}
