package dak.ant.taskdefs;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.tools.ant.BuildException;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import dak.ant.typedefs.S3ObjectSet;
import dak.ant.typedefs.S3ObjectWrapper;
import dak.ant.util.S3BucketScanner;

public class S3Delete extends AWSTask { 
    
    // INSTANCE VARIABLES

    private String bucket;
    private String prefix;
    private Pattern filePattern;

    private List<S3ObjectSet> objectlists = new ArrayList<S3ObjectSet>();

    // PROPERTIES

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public void setFileRegex(String regex) {
        filePattern = Pattern.compile(regex);
    }

    public S3ObjectSet createObjectSet() {
        S3ObjectSet objectset = new S3ObjectSet();

        objectlists.add(objectset);

        return objectset;
    }

    public S3ObjectSet createS3ObjectSet(S3ObjectSet list) {
        S3ObjectSet objectset = new S3ObjectSet();

        objectlists.add(objectset);

        return objectset;
    }

    // IMPLEMENTATION

    @Override
    public void execute() throws BuildException {
        checkParameters();

        AWSCredentials credentials = new AWSCredentials(accessId, secretKey);

        try {
            S3Service s3 = new RestS3Service(credentials);
            List<S3Object> list = new ArrayList<S3Object>();

            // ... match on regular expression

            if (filePattern != null) {
                S3Object[] objects;

                if (prefix != null)
                    objects = s3.listObjects(bucket, prefix, null);
                else
                    objects = s3.listObjects(bucket);

                for (S3Object object : objects) {
                    if (objectMatches(object)) {
                        list.add(object);
                    }
                }
            }

            // ... match on object sets

            for (S3ObjectSet objectset : objectlists) {
                S3BucketScanner scanner = objectset.getBucketScanner(s3);
                S3Bucket bucket = new S3Bucket(scanner.getBucket());
                S3ObjectWrapper[] objects = scanner.getIncludedObjects();

                for (S3ObjectWrapper object : objects) {
                    list.add(new S3Object(bucket, object.getKey()));
                }
            }

            // ... delete objects in list

            if (list.isEmpty())
                log("Delete list is empty - nothing to do.");
            else
                log("Deleting " + list.size() + " objects");

            for (S3Object object : list) {
                s3.deleteObject(object.getBucketName(), object.getKey());

                if (verbose)
                    log("Deleted '" + object.getBucketName() + "/"
                            + object.getKey() + "'");
            }
        } catch (Exception x) {
            throw new BuildException(x);
        }
    }

    private boolean objectMatches(S3Object object) {
        String toCompare = (prefix == null) ? object.getKey() : object.getKey()
                .substring(prefix.length());

        return (filePattern == null) ? true : filePattern.matcher(toCompare)
                .matches();
    }
}
