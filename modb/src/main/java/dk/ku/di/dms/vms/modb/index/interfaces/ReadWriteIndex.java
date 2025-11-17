package dk.ku.di.dms.vms.modb.index.interfaces;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.index.AbstractIndex;
import dk.ku.di.dms.vms.modb.storage.record.RecordBufferContext;

public abstract class ReadWriteIndex<K> extends AbstractIndex<K> {

    public ReadWriteIndex(Schema schema, int[] columnsIndex) {
        super(schema, columnsIndex);
    }

    public abstract void insert(K key, Object[] record);

    public abstract void update(K key, Object[] record);

    public abstract void delete(K key);

    public abstract Object[] lookupByKey(K key);

    public void upsert(K key, Object[] record) {
        if(this.exists(key)) {
            this.update(key, record);
            return;
        }
        this.insert(key, record);
    }

    public RecordBufferContext RecordBufferContext() { return null; }
    public void lock(){
        throw new RuntimeException("Not supported.");
    }

    public void unlock(){
        throw new RuntimeException("Not supported.");
    }

    public void reset() {
        throw new RuntimeException("Not supported.");
    }

    // flush updates
    public void flush(){
        throw new RuntimeException("Not supported.");
    }

}
