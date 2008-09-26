package dak.ant.taskdefs;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.Vector;

import javax.activation.MimetypesFileTypeMap;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
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
	private static final long MONTH_SECONDS = 60 * 60 * 24 * 30L;
	private static final long YEAR_MILLISECONDS = 365 * 24 * 60 * 60 * 1000L;

	protected String bucket;
	protected String prefix = "";
	protected boolean publicRead = false;
	protected Vector<FileSet> filesets = new Vector<FileSet>();
	private AccessControlList bucketAcl;
	private boolean cacheNeverExpires = false;

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

	public void setCacheNeverExpires(boolean cacheNeverExpires) {
		this.cacheNeverExpires = cacheNeverExpires;
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
        	for (FileSet fs : filesets) {
				try {
					DirectoryScanner ds = fs.getDirectoryScanner(getProject());
					String[] files = ds.getIncludedFiles();
					File d = fs.getDir(getProject());

					if (files.length > 0) {
						log("copying " + files.length + " files from "
							+ d.getAbsolutePath());
						for (int j = 0; j < files.length; j++) {
							copyFile(s3, bucket, d, files[j]);
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

	private void copyFile(RestS3Service s3, S3Bucket b, File d, String file) throws Exception {
		String key = (prefix+file).replace('\\', '/');
		if (verbose) {
			log("about to copy : "+key);
		}
        S3Object obj = new S3Object(b, key);
		if (publicRead) {
			obj.setAcl(bucketAcl);
		}
		if (cacheNeverExpires) {
			obj.addMetadata("Expires",
				formatDate(new Date(new Date().getTime() + YEAR_MILLISECONDS)));
			obj.addMetadata("Cache-Control", "public, max-age=" + (MONTH_SECONDS * 3));
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

	private String formatDate(Date date) {
		DateFormat httpDateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
		httpDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		return httpDateFormat.format(date);
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
