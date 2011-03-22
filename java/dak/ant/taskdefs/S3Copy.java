package dak.ant.taskdefs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.types.LogLevel;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import dak.ant.types.S3File;
import dak.ant.types.S3FileSetX;

/** Ant task do do bucket-to-bucket copy.
  *  
  * @author Chris Stewart
  *
  */

// TS: Removed non-fileset copy - no longer required since FileName selector supports regular expressions 
//     as of Ant 1.8 and S3FileSet now supports include/exclude patterns.
//     
// TS: Added automatic bucket creation.

public class S3Copy extends AWSTask 
       { // INSTANCE VARIABLES
    
         private String           bucket;
         private List<S3FileSetX> filesets = new ArrayList<S3FileSetX>();
         private boolean          dummyRun = false;

         // PROPERTIES

         public void setBucket(String bucket)  
                { this.bucket = bucket;
                }

         public S3FileSetX createS3FileSet() 
                { S3FileSetX fileset = new S3FileSetX();

                  filesets.add(fileset);

                  return fileset;
                }
         
         public void setDummyRun(boolean enabled)
                { this.dummyRun = enabled;
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

                       if (list.isEmpty())
                          { log("Copy list is empty - nothing to do.");
                            return;
                          }
                       
                       // ... create bucket
                       
                      if (service.getBucket(bucket) == null) 
                         { log("Bucket '" + bucket + "' does not exist ! Creating ...",LogLevel.WARN.getLevel());
                           service.createBucket(new S3Bucket(bucket));
                         }

                       // ... copy objects in list

                       log("Copying " + list.size() + " objects");

                       for (S3File file: list) 
                           { S3Object object = new S3Object(file.key);

                             if (dummyRun)
                                { log("DUMMY RUN: copied '" + file.bucket + "::" + file.key + "' to '" + bucket + "::" + object.getKey() + "'");
                                }
                                else
                                { service.copyObject(file.bucket,file.key,bucket,object,true);
                                    
                                  if (verbose)
                                    log("Copied '" + file.bucket + "::" + file.key + "' to '" + bucket + "::" + object.getKey() + "'");
                                }
                           }
                     }
                  catch(BuildException x)
                     { throw x;
                     }
                  catch(ServiceException x)
                     { throw new BuildException(x.getErrorMessage());
                     }
                  catch (Exception x) 
                     { throw new BuildException(x);
                     }
                }

         @Override
         protected void checkParameters() throws BuildException 
                   { super.checkParameters();
                 
                     if (bucket == null)
                        { throw new BuildException("'bucket' task attribute must be set");
                        }
                   }
       }
