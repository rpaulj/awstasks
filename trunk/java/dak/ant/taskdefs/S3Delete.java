package dak.ant.taskdefs;

import java.util.regex.Pattern;

import org.apache.tools.ant.BuildException;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class S3Delete extends AWSTask {
    private String bucket;
    private String prefix;
    private Pattern filePattern;
    
    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
    
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public void setFileRegex(String regex) {
        filePattern = Pattern.compile(regex);
    }

    @Override
    public void execute() throws BuildException {
        checkParameters();
        AWSCredentials credentials = new AWSCredentials(accessId, secretKey);
        try {
            S3Service s3 = new RestS3Service(credentials);
            S3Bucket bucket = new S3Bucket(this.bucket);
            S3Object[] objectListing;
            if (prefix != null)
                objectListing = s3.listObjects(bucket, prefix, null);
            else
                objectListing = s3.listObjects(bucket);
            for (S3Object object : objectListing) {
                if (objectMatches(object)) {
                    s3.deleteObject(bucket, object.getKey());
                    if (verbose)
                        log("deleted " + object.getKey());
                }
            }
        }
        catch (Exception e) {
            throw new BuildException(e);
        }
    }
    
    private boolean objectMatches(S3Object object) {
        String toCompare = prefix == null ? 
                object.getKey() : object.getKey().substring(prefix.length());
        if (filePattern == null)
            return true;
        else
            return filePattern.matcher(toCompare).matches();
    }
}
