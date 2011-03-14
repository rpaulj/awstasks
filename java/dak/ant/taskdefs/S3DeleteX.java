package dak.ant.taskdefs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import dak.ant.types.S3File;
import dak.ant.types.S3FileSetY;
import dak.ant.types.S3FileSetX;
import dak.ant.types.S3ObjectWrapper;
import dak.ant.util.S3BucketScanner;

/** Ant task to delete S3 objects selected using an S3FileSet.
 * 
 * @author Tony Seebregts
 *
 */
public class S3DeleteX extends AWSTask 
       { // INSTANCE VARIABLES

         private String           bucket;
         private List<S3FileSetX> filesets = new ArrayList<S3FileSetX>  ();

         // PROPERTIES

         public void setBucket(String bucket) 
                { this.bucket = bucket;
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
                       List<S3Object> list        = new ArrayList<S3Object>();

                       // ... match on object sets

                       for (S3FileSetX fileset: filesets)
                           { Iterator<S3File> ix = fileset.iterator(credentials); 
                         
                             while (ix.hasNext()) 
                                   { S3File   file   = ix.next();
                                     String   key    = file.getName();
                                     S3Object object = new S3Object(key);
         
                                     list.add(object);
                                   }  
                           }
                       
//                       // ... match on file sets
//
//                       for (S3FileSetY fileset: filesets) 
//                           { S3BucketScanner   scanner = fileset.getBucketScanner(service);
//                             S3Bucket          bucket  = new S3Bucket(scanner.getBucket());
//                             S3ObjectWrapper[] objects = scanner.getIncludedObjects();
//
//                             for (S3ObjectWrapper object: objects) 
//                                 { list.add(new S3Object(bucket, object.getKey()));
//                                 }
//                             
//                           }

                       // ... delete objects in list

                       if (list.isEmpty())
                          log("Delete list is empty - nothing to do.");
                          else
                          log("Deleting " + list.size() + " objects");

                       for (S3Object object: list) 
                           { //s3.deleteObject(object.getBucketName(), object.getKey());

                             if (verbose)
                               log("Deleted '" + object.getBucketName() + "/" + object.getKey() + "'");
                           }
                     } 
                  catch (Exception x)
                     { throw new BuildException(x);
                     }
                }

       }
