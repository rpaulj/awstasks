package dak.ant.taskdefs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.tools.ant.BuildException;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import dak.ant.types.S3File;
import dak.ant.types.S3FileSetX;

/** Ant task to delete S3 objects selected using an S3FileSet.
 * 
 * @author Tony Seebregts
 *
 */
public class S3DeleteX extends AWSTask 
       { // INSTANCE VARIABLES

         private boolean          dummyRun = false;
         private List<S3FileSetX> filesets = new ArrayList<S3FileSetX>  ();

         // PROPERTIES

         public void setDummyRun(boolean enabled)
                { this.dummyRun = enabled;
                }
         
         public S3FileSetX createS3FileSetX() 
                { S3FileSetX fileset = new S3FileSetX();

                  filesets.add(fileset);

                  return fileset;
                }

         // IMPLEMENTATION

         @Override
         public void execute() throws BuildException 
                { checkParameters();

                  try 
                     { AWSCredentials credentials = new AWSCredentials(accessId, secretKey);
                       S3Service      service     = new RestS3Service(credentials);
                       Set<S3File>    list        = new ConcurrentSkipListSet<S3File>();

                       // ... match on filesets

                       for (S3FileSetX fileset: filesets)
                           { Iterator<S3File> ix = fileset.iterator(service); 
                         
                             while (ix.hasNext()) 
                                   { list.add(ix.next());
                                   }  
                           }
                       
                       // ... delete objects in list

                       if (list.isEmpty())
                          log("Delete list is empty - nothing to do.");
                          else
                          log("Deleting " + list.size() + " objects");

                       Map<String,S3Bucket> buckets = new HashMap<String,S3Bucket>();
                       S3Bucket             bucket;
                       S3Object             object;
                       
                       for (S3File file: list) 
                           { if ((bucket = buckets.get(file.bucket)) == null)
                                { bucket = new S3Bucket(file.bucket);
                                
                                  buckets.put(file.bucket,bucket);
                                }
                           
                             object = new S3Object(bucket,file.key);

                             if (dummyRun)
                                { log("DUMMY RUN: deleted '[" + object.getBucketName() + "][" + object.getKey() + "'");
                                
                                }
                                else
                                { //s3.deleteObject(object.getBucketName(), object.getKey());

                                  if (verbose)
                                    log("Deleted '[" + object.getBucketName() + "][" + object.getKey() + "'");
                                }
                           }
                     } 
                  catch (Exception x)
                     { throw new BuildException(x);
                     }
                }

       }
