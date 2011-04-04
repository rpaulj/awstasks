package dak.ant.taskdefs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.activation.MimetypesFileTypeMap;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.LogLevel;

import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.FileComparer;
import org.jets3t.service.utils.FileComparerResults;

/** Implements an Ant task with the JetS3t synchronise functionality.
  * 
  * @author Tony Seebregts
  */
public class S3Synchronize extends AWSTask 
       { // CONSTANTS

         private enum DIRECTION { UPLOAD("upload"), 
                                  DOWNLOAD("download");

                                  private final String code;

                                  private DIRECTION(String code) 
                                          { this.code = code;
                                          }

                                  private static DIRECTION parse(String code) 
                                          { for (DIRECTION direction: values()) 
                                                { if (direction.code.equalsIgnoreCase(code))
                                                     return direction;
                                                }

                                            return null;
                                          }
                                };

         // INSTANCE VARIABLES

         private String               bucket;
         private String               prefix            = "";
         private boolean              publicRead        = false;
         private List<FileSet>        filesets          = new ArrayList<FileSet>();
         private boolean              cacheNeverExpires = false;
         private MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
         private String               mimeTypesFile;
         private AccessControlList    acl;

         private DIRECTION direction;
         private boolean   includeDirectories = true;
         private boolean   dummyRun = false;
         private boolean   delete   = true;
         private boolean   revert   = true;

         // PROPERTIES

         public void setBucket(String bucket) 
                { this.bucket = bucket;
                }

         public void setPrefix(String prefix) 
                { this.prefix = prefix;
                }

         public void setPublicRead(boolean on) 
                { this.publicRead = on;
                }

    public void addFileset(FileSet set) {
        filesets.add(set);
    }

    public void setCacheNeverExpires(boolean cacheNeverExpires) {
        this.cacheNeverExpires = cacheNeverExpires;
    }

    public void setMimeTypesFile(String mimeTypesFile) {
        this.mimeTypesFile = mimeTypesFile;
    }

    public void setIncludeDirectories(boolean enabled) {
        this.includeDirectories = enabled;
    }

    public void setDirection(String direction) {
        this.direction = DIRECTION.parse(direction);
    }

         public void setDummyRun(boolean enabled) 
                { this.dummyRun = enabled;
                }

         public void setDelete(boolean enabled) 
                { this.delete = enabled;
                }

         public void setRevert(boolean enabled) 
                { this.revert = enabled;
                }

         // IMPLEMENTATION

         /** Check that all required attributes have been set and warns if the fileset
           * list is empty.
           * 
           * @since Ant 1.5
           * @exception BuildException
           *                if an error occurs
           */
         @Override
         protected void checkParameters() throws BuildException 
                   { super.checkParameters();

                     if (direction == null) 
                        { throw new BuildException("Invalid SYNCHRONIZE direction. Valid values are 'upload' or 'download'");
                        }

                     if (filesets == null) 
                        { log("No fileset specified, doing nothing", LogLevel.WARN.getLevel());
                          return;
                        }
                   }

         /**
          * Validates the parameters and then synchronizes the S3 and file specified
          * in the filesets.
          * 
          * @throws BuildException
          *             Thrown if the parameters are invalid or the synchronize
          *             failed.
          */
         @Override
         public void execute() throws BuildException {
             checkParameters();

             try { // ... initialise

                 AWSCredentials credentials = new AWSCredentials(accessId, secretKey);
                 RestS3Service s3 = new RestS3Service(credentials);
                 S3Bucket bucket = new S3Bucket(this.bucket);

                 if (publicRead) {
                     acl = s3.getBucketAcl(bucket);
                     acl.grantPermission(GroupGrantee.ALL_USERS,
                             Permission.PERMISSION_READ);
                 }

                 if (mimeTypesFile != null)
                     mimeTypesMap = new MimetypesFileTypeMap(mimeTypesFile);
                 else
                     mimeTypesMap = new MimetypesFileTypeMap();

                 // ... synchronise directory

                 try {
                     for (FileSet fs : filesets) {
                         DirectoryScanner ds = fs.getDirectoryScanner(getProject());
                         File root = fs.getDir(getProject());
                         String[] subdirs = ds.getIncludedDirectories();
                         String[] files = ds.getIncludedFiles();
                         List<File> list = new ArrayList<File>();

                         if (includeDirectories) {
                             for (String dir : subdirs) {
                                 list.add(new File(root, dir));
                             }
                         }

                         for (String file : files) {
                             list.add(new File(root, file));
                         }

                         switch (direction) {
                         case UPLOAD:
                             upload(s3, bucket, root, list.toArray(new File[0]));
                             break;

                         case DOWNLOAD:
                             download(s3, bucket, root, list.toArray(new File[0]));
                             break;
                         }
                     }

                 } catch (BuildException x) {
                     if (failOnError)
                         throw x;

                     log("Error synchronizing files with Amazon S3 ["
                             + x.getMessage() + "]", LogLevel.ERR.getLevel());
                 }
             } catch (Exception x) {
                 throw new BuildException(x);
             }
         }

         private void upload(RestS3Service service,S3Bucket bucket,File root,File[] list) throws Exception
                 { // ... create bucket if necessary

                   if (service.getBucket(bucket.getName()) == null) 
                      { log("Bucket '" + bucket.getName() + "' does not exist ! Creating ...",LogLevel.WARN.getLevel());
                        service.createBucket(bucket);
                      }

                   // ... build change list

                   FileComparer              fc      = FileComparer.getInstance();
                   Map<String,File>          files   = buildFileMap     (root,list,prefix);
                   Map<String,StorageObject> objects = fc.buildObjectMap(service,bucket.getName(),"",false,null);
                   FileComparerResults       rs      = fc.buildDiscrepancyLists(files, objects);
                   AccessControlList         acl     = publicRead ? this.acl : null;

                   // ... synchronize

                   for (String key : rs.onlyOnClientKeys) 
                       { File   file        = files.get(key);
                         String contentType = mimeTypesMap.getContentType(file);

                         if (dummyRun)
                            log(DUMMY_RUN + " Added: [" + key + "]");
                            else 
                            { if (verbose)
                                 log("Added: " + "[" + key + "][" + file + "]");

                              upload(service,bucket,acl,cacheNeverExpires,key,file,contentType);
                            }
                       }

                   for (String key : rs.updatedOnClientKeys) 
                       { File   file        = files.get(key);
                         String contentType = mimeTypesMap.getContentType(file);

                         if (dummyRun)
                           log(DUMMY_RUN + " Updated: [" + key + "]");
                           else 
                           { if (verbose)
                                log("Updated: " + "[" + key + "][" + file + "]");

                             upload(service,bucket,acl,cacheNeverExpires,key,file,contentType);
                           }
                       }

                   for (String key: rs.onlyOnServerKeys) 
                       { if (dummyRun)
                            log(DUMMY_RUN + " Deleted: [" + key + "]");
                            else if (delete) 
                            delete(service, bucket, key, "Deleted: ");
                       }

                   for (String key: rs.updatedOnServerKeys) 
                       { File   file        = files.get(key);
                         String contentType = mimeTypesMap.getContentType(file);

                         if (dummyRun)
                           log(DUMMY_RUN + " Reverted: [" + key + "]");
                           else if (revert)
                           { if (verbose)
                               log("Reverted: " + "[" + key + "][" + file + "]");

                             upload(service,bucket,acl,cacheNeverExpires,key,file,contentType);
                           }
                       }
                 }

         private void download(RestS3Service service,S3Bucket bucket,File root,File[] list) throws Exception 
                 { // ... build change list

                   FileComparer              fc      = FileComparer.getInstance();
                   Map<String,File>          files   = buildFileMap     (root,list,prefix);
                   Map<String,StorageObject> objects = fc.buildObjectMap(service,bucket.getName(),"",false,null);
                   FileComparerResults       rs      = fc.buildDiscrepancyLists(files, objects);

                   // ... synchronize

                   for (String key: rs.onlyOnServerKeys) 
                       { if (dummyRun)
                            log(DUMMY_RUN + " Added: [" + key + "]");
                            else
                            download(service,bucket, key, new File(root, key), "Added:");
                       }

                   for (String key: rs.updatedOnServerKeys)
                       { if (dummyRun)
                            log(DUMMY_RUN + " Updated: [" + key + "]");
                            else
                            download(service,bucket,key,files.get(key),"Updated: ");
                       }

                   for (String key: rs.onlyOnClientKeys)
                       { if (dummyRun)
                            log(DUMMY_RUN + " Deleted: [" + key + "]");
                            else if (delete)
                            delete(files.get(key),"Deleted: ");
                       }

                   for (String key: rs.updatedOnClientKeys) 
                       { if (dummyRun)
                            log(DUMMY_RUN + " Reverted: [" + key + "]");
                            else if (revert)
                            download(service, bucket, key, files.get(key), "Reverted: ");
                       }
                 }

         /** Downloads a file from an S3 bucket.
           * 
           * @param s3     Initialised S3Service.
           * @param key    S3 object key for file to download.
           * @param file  Local file to which to download.
           * @param action Action text for log message.
           * 
           * @throws Exception Thrown if the file upload fails for any reason.
           */
         private void download(RestS3Service s3,S3Bucket bucket, String key,File file,String action) throws Exception
                 { if (verbose) 
                      { log(action + "[" + key + "][" + file + "]");
                      }

                   // ... get object

                   S3Object object = s3.getObject(bucket.getName(), key);

                   // ... directory ?

                   if (!object.isMetadataComplete() && (object.getContentLength() == 0) && !object.isDirectoryPlaceholder()) 
                      { log("Skipping '" + key + "' - may or may not be a directory");
                        return;
                      }

                   if (object.isMetadataComplete() && (object.getContentLength() == 0) && object.isDirectoryPlaceholder()) 
                      { log("Creating directory '" + key + "'");
                        file.mkdirs();
                        return;
                      }

                   if (file.exists() && file.isDirectory()) 
                      { log("Warning: file '" + key + "' exists as a directory");
                        return;
                      }

                   // ... download file

                   byte[]       buffer = new byte[16384];
                   InputStream  in     = null;
                   OutputStream out    = null;
                   int          N;

                   try
                      { file.getParentFile().mkdirs();

                        in = object.getDataInputStream();
                        out = new FileOutputStream(file);

                        while ((N = in.read(buffer)) != -1) 
                              { out.write(buffer,0,N);
                              }
                      }
                   finally
                      { close(in);
                        close(out);
                      }
                 }

         /** Deletes a file from an S3 bucket.
           * 
           * @param s3     Initialised S3Service.
           * @param bucket Initialised S3Bucket.
           * @param key    S3 object key to delete.
           * @param action Action text for log message.
           * 
           * @throws Exception
           *             Thrown if the file upload fails for any reason.
           */
         private void delete(RestS3Service s3,S3Bucket bucket,String key,String action) throws Exception 
                 { if (verbose)
                      { log(action + "[" + key + "]");
                      }

                   s3.deleteObject(bucket, key);
                 }

         /** Deletes a local file.
           * 
           * @param file   Local file to delete.
           * @param action Action text for log message.
           * 
           * @throws Exception Thrown if the file upload fails for any reason.
           */
         private void delete(File file,String action) throws Exception
                 { if (verbose) 
                      { log(action + "[" + file + "]");
                      }

                   if (file.exists())
                      file.delete();
                 }
       }
