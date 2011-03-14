package dak.ant.types;

import java.io.File;

import org.jets3t.service.model.S3Object;

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
       { private final String bucket;
         private final String key;
         private final int    hashcode;
         
         private long         lastModified;
   
         public S3File(String bucket,String key)
                { super(key);
                 
                  this.bucket   = bucket;
                  this.key      = key;
                  this.hashcode = (bucket + "/" + key).hashCode();
                }
         
         
         public S3File(S3Object object)
                { super(object.getKey());
                 
                  this.bucket       = object.getBucketName();
                  this.key          = object.getKey();
                  this.hashcode     = (bucket + "/" + key).hashCode();
                  this.lastModified = object.getLastModifiedDate().getTime();
                }

         @Override
         public boolean isDirectory()
                { return false; // currently no such thing as a directory.
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
         
         public String getKey()
                { return key;
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
