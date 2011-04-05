package dak.ant.types;

import java.io.File;

import org.jets3t.service.model.S3Object;

import dak.ant.taskdefs.AWSTask;

/**
 * Attempt to make S3 objects compatible with ant file selectors.
 * This could probably be done a lot better. One thing to explore would
 * be the URI constructor - this might allow bucket to be used as part
 * of the file path which might make some of the S3 operations easier
 * to handle? Or maybe harder!
 * 
 * It might be here that you could plug in a File System implementation that
 * uses any of a number of naming schemes to simulate directories within S3 -
 * a number of tools do this and it'd be nice to be able to read their
 * output. 
 * 
 * I should probably handle isAbsolute some how.
 * @author chris
 *
 */
@SuppressWarnings("serial")
public class S3File extends File 
       { // INSTANCE VARIABLES
    
         private String  bucket;
         private String  key;
         private int     hashcode;
         private long    lastModified = 0;
         private long    length       = 0;
         private boolean exists       = false;
         private boolean isDirectory  = true;
   
         // CLASS METHODS
         
         private static String clean(String string)
                 { return string == null ? "" : string;
                 }
         
         // CONSTRUCTORS
         
         /** Default constructor for use by <code>S3FileSet.createS3File</code>. Be nice to it !!
           * 
           */
         public S3File()
                { super("");
                }
   
         public S3File(String bucket,String key)
                { super(key);
                 
                  this.bucket   = clean(bucket);
                  this.key      = clean(key);
                  this.hashcode = (this.bucket + "/" + this.key).hashCode();
                }
         
         
         public S3File(S3Object object)
                { super(object.getKey());
                 
                  this.bucket       = clean(object.getBucketName());
                  this.key          = clean(object.getKey());
                  this.hashcode     = (this.bucket + "/" + this.key).hashCode();
                  this.lastModified = object.getLastModifiedDate().getTime();
                  this.length       = object.getContentLength();
                  this.exists       = true;
                  this.isDirectory  = AWSTask.isDirectory(object);
                }

         // PROPERTIES
         
         public void setBucket(String bucket)
                { this.bucket = clean(bucket);
                  this.hashcode = (this.bucket + "/" + this.key).hashCode();
                }
         
         public void setKey(String key)
                { this.key      = clean(key);
                  this.hashcode = (this.bucket + "/" + this.key).hashCode();
                }
         
         public String getBucket()
                { return bucket;
                }
         
         public String getKey()
                { return key;
                }

         // *** File ***
         
         /** Only valid if the file has been uploaded with either isDirectoryPlaceHolder set and/or a contentType of 
           * application/x-directory.
           *  
           */
         @Override
         public boolean isDirectory()
                { return isDirectory; 
                }
   
         @Override
         public boolean setLastModified(long time) 
                { lastModified = time;
                
                  return true;
                }
   
         @Override
         public long lastModified()
                { return lastModified;
                }
         
         @Override
         public long length() 
                { return length;
                }
         
         @Override
         public boolean exists()
                { return exists;
                }

         // *** Object ***

         @Override
         public boolean equals(Object object)
                { if (object instanceof S3File)
                     if (bucket.equals(((S3File) object).bucket))
                        if (key.equals(((S3File) object).key))
                           return true;

                  return false;
                }

         @Override
         public int hashCode() 
                { return hashcode;
                }
       }
