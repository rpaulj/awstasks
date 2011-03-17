package dak.ant.taskdefs;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
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

/** Ant task to list the S3 objects selected using nested S3FileSet's. Mostly implemented
  * to test the various selectors but may find other uses.
  * 
  * @author Tony Seebregts
  *
  */
public class S3List extends AWSTask 
       { // INSTANCE VARIABLES

         private String           format = "%s::%s";   
         private boolean          append = false;
         private String           outfile;
         private List<S3FileSetX> filesets = new ArrayList<S3FileSetX>  ();

         // PROPERTIES

         public void setOutFile(String outfile) 
                { this.outfile = outfile;
                }

         public void setFormat(String format) 
                { this.format = format;
                }

         public void setAppend(boolean append) 
                { this.append = append;
                }

         public S3FileSetX createS3FileSet() 
                { S3FileSetX fileset = new S3FileSetX();

                  filesets.add(fileset);

                  return fileset;
                }

         // IMPLEMENTATION

         @Override
         public void execute() throws BuildException 
                { checkParameters();

                  PrintWriter writer = null;

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

                       // ... open output file
                       
                       if (outfile != null)
                          writer = new PrintWriter(new FileWriter(new File(outfile),append));
                       
                       // ... print objects in list

                       Map<String,S3Bucket> buckets = new HashMap<String,S3Bucket>();
                       S3Bucket             bucket;
                       S3Object             object;

                       log("Listing " + list.size() + " objects");

                       for (S3File file: list) 
                           { // ... re-use buckets just in case they ever become heavyweight objects
                           
                             if ((bucket = buckets.get(file.bucket)) == null)
                                { bucket = new S3Bucket(file.bucket);
                                
                                  buckets.put(file.bucket,bucket);
                                }
                           
                             // ... go dog go !
                             
                             object = new S3Object(bucket,file.key);

                             if ((outfile == null) || verbose)
                                log(String.format(format,object.getBucketName(),object.getKey()));
                               
                             if (writer != null)
                                { writer.println(String.format(format,object.getBucketName(),object.getKey()));
                                }
                           }
                     } 
                  catch (Exception x)
                     { throw new BuildException(x);
                     }
                  finally
                     { close(writer);
                     }
                }

       }
