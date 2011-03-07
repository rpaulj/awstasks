package dak.ant.typedefs;

import org.apache.tools.ant.types.DataType;
import org.jets3t.service.model.S3Object;

public class S3ObjectWrapper extends DataType implements Comparable<S3ObjectWrapper> {
    
    // INSTANCE VARIABLES
    
    private String key;

    // CONSTRUCTORS

    public S3ObjectWrapper() {
    }

    public S3ObjectWrapper(S3Object object) {
        setKey(object.getKey());
    }

    // PROPERTIES

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    // *** Comparable ***

    @Override
    public int compareTo(S3ObjectWrapper object) {
        return key.compareTo(object.key);
    }

    // *** Object ***

    @Override
    public boolean equals(Object object) {
        if (object instanceof S3ObjectWrapper)
            if (key.equals(((S3ObjectWrapper) object).key))
                return true;

        return false;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
