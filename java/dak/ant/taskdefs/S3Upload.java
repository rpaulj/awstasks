package dak.ant.taskdefs;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.Vector;

import javax.activation.MimetypesFileTypeMap;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.taskdefs.MatchingTask;
import org.apache.tools.ant.types.FileSet;

import org.jets3t.service.S3ServiceException;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.acl.GroupGrantee;
import org.jets3t.service.acl.Permission;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/**
 * <p>This class provides basic S3 actions as an Ant task</p>
 * 
 * @author D. Kavanagh
 */
public class S3Upload extends AWSTask {
	protected String bucket;
	protected String prefix = "";
	protected boolean publicRead = false;
	protected Vector filesets = new Vector();
	private AccessControlList bucketAcl;

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public void setPublicRead(boolean on) {
		this.publicRead = on;
	}

	public void addFileset(FileSet set) {
		filesets.addElement(set);
	}

	public void execute() throws BuildException {
		checkParameters();

		AWSCredentials credentials = new AWSCredentials(accessId, secretKey);
		try {
			RestS3Service s3 = new RestS3Service(credentials);
			S3Bucket bucket = new S3Bucket(this.bucket);
			if (publicRead) {
				bucketAcl = s3.getBucketAcl(bucket);
				bucketAcl.grantPermission(GroupGrantee.ALL_USERS, Permission.PERMISSION_READ);
			}
        	for (int i = 0; i < filesets.size(); i++) {
            	FileSet fs = (FileSet) filesets.elementAt(i);
				try {
					DirectoryScanner ds = fs.getDirectoryScanner(getProject());
					String[] files = ds.getIncludedFiles();
					String[] dirs = ds.getIncludedDirectories();
					File d = fs.getDir(getProject());
					if (files.length > 0) {
						log("copying " + files.length + " files from "
							+ d.getAbsolutePath());
						for (int j = 0; j < files.length; j++) {
							copyFile(s3, bucket, d, files[j]);
						}
					}

					if (dirs.length > 0) {
						int dirCount = 0;
						for (int j = dirs.length - 1; j >= 0; j--) {
							log("copying dir " + d.getAbsolutePath() +", "+ dirs[j]);
							processDirectory(s3, bucket, d, dirs[j]);
			//                    dirCount++;
						}

						// TODO: need an accurate count?
						if (dirCount > 0) {
							log("Copied " + dirCount + " director"
								+ (dirCount == 1 ? "y" : "ies")
								+ " from " + d.getAbsolutePath());
						}
					}
				} catch (BuildException be) {
					// directory doesn't exist or is not readable
					if (failOnError) {
						throw be;
					} else {
						log("Could not copy file(s) to Amazon S3");
						log(be.getMessage());
					}
				}
			}
		} catch (Exception e) {
			throw new BuildException(e);
		}
	}

	private void processDirectory(RestS3Service s3, S3Bucket b, File d, String subdir) throws Exception {
		File [] files = new File(d, subdir).listFiles();
		for (int i=0; i<files.length; i++) {
			if (files[i].isDirectory()) {
				processDirectory(s3, b, d, files[i].getPath());
			}
			else {
				copyFile(s3, b, d, files[i].getPath());
			}
		}
	}

	private void copyFile(RestS3Service s3, S3Bucket b, File d, String file) throws Exception {
		String key = (prefix+file).replace('\\', '/');
		if (verbose) {
			log("about to copy : "+key);
		}
        S3Object obj = new S3Object(b, key);
		if (publicRead) {
			obj.setAcl(bucketAcl);
		}
		File f = new File(d, file);
        obj.setContentLength(f.length());
		obj.setContentType(new MimetypesFileTypeMap().getContentType(f));
        try {
            obj.setDataInputFile(f);
            obj = s3.putObject(bucket, obj);
        } catch (S3ServiceException e) {
            throw e;
        }
		if (verbose) {
			log("copied : "+key);
		}
	}

	/**
     * Check that all required attributes have been set and nothing
     * silly has been entered.
     *
     * @since Ant 1.5
     * @exception BuildException if an error occurs
     */
    protected void checkParameters() throws BuildException {
		super.checkParameters();
		if (filesets == null) {
			System.err.println("No fileset was provided, doing nothing");
			return;
		}
    }
}
