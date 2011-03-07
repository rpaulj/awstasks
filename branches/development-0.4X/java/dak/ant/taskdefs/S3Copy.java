package dak.ant.taskdefs;

import java.io.File;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.tools.ant.BuildException;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import dak.ant.types.S3FileSet;

/**
 * N.B. Only bucket to bucket copy as this is all i need, but 
 * It should be extendible if/when required. 
 * @author chris
 *
 */
public class S3Copy extends S3FileSetTask {
    private String toBucket;
    private String fromBucket;
    private String prefix;
    private Pattern filePattern;
    
    public void setToBucket(String bucket) {
        this.toBucket = bucket;
    }
    
    public void setFromBucket(String bucket) {
        this.fromBucket = bucket;
    }
    
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public void setFileRegex(String regex) {
       this.filePattern = Pattern.compile(regex);
    }

    @Override
    public void execute() throws BuildException {
        checkParameters();
        try {
           if( isFileSetMode( ) )
              for ( S3FileSet fileSet : fileSets ) {
                 doFileSetCopy( fileSet );               
              }
           else   
             doNonFileSetCopy( );
        }
        catch (Exception e) {
            throw new BuildException(e);
        }
    }

   @SuppressWarnings("unchecked")
   private void doFileSetCopy( S3FileSet fileSet ) throws ServiceException {
      
      AWSCredentials credentials = fileSet.getCredentials( );
      S3Service s3 = new RestS3Service(credentials);
       
      for ( Iterator<File> i = fileSet.iterator( ); i.hasNext( ); ) {
         File file = i.next( );
         
         String key = file.getName( );
         
         S3Object object = new S3Object( key );
         
         s3.copyObject( fileSet.getBucket( ), key, toBucket, object, true );
         
         if (verbose)
            log("copied " + object.getKey());
      }  
   }

   private void doNonFileSetCopy( ) throws ServiceException {
      
      AWSCredentials credentials = new AWSCredentials(accessId, secretKey);
      S3Service s3 = new RestS3Service(credentials);
      S3Bucket fromBucket = new S3Bucket(this.fromBucket);
      
      S3Object[] objectListing;
      if (prefix != null)
          objectListing = s3.listObjects(fromBucket, prefix, null);
      else
          objectListing = s3.listObjects(fromBucket);
      for (S3Object object : objectListing) {
          if (objectMatches(object)) {
             s3.copyObject( this.fromBucket, object.getKey( ), toBucket, object, true );
              if (verbose)
                  log("copied " + object.getKey());
          }
      }
   }
    
    protected void checkParameters() throws BuildException {
           
       if( toBucket == null ) {
          throw new BuildException("toBucket must be set");
       }
      
       if( !isFileSetMode() ) {
          super.checkParameters( );
          
          if( fromBucket == null ) {
             throw new BuildException("fromBucket must be set");
          }
          
          return;
       }
       // S3FileSet(s) given
       // S3FileSet contains all of these
       if( accessId != null ) {
          throw new BuildException("cannot set s3FileSet and accessId");
       }
       if( secretKey != null ) {
          throw new BuildException("cannot set s3FileSet and secretKey");
       }
       if( fromBucket != null ) {
          throw new BuildException("cannot set s3FileSet and bucket");
       }
       if( prefix != null ) {
          throw new BuildException("cannot set s3FileSet and prefix");
       }
       if( filePattern != null ) {
          throw new BuildException("cannot set s3FileSet and filePattern");
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
