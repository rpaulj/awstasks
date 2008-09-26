package dak.ant.taskdefs;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.MatchingTask;

/**
 * <p>This class provides basic S3 actions as an Ant task</p>
 * 
 * @author D. Kavanagh
 */
public abstract class AWSTask extends MatchingTask {

	protected boolean verbose = false;
	protected boolean failOnError = false;
	protected String accessId;
	protected String secretKey;

	public AWSTask() {
		super();
	}
	
	public void setAccessId(String accessId) {
		this.accessId = accessId;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public void setFailOnError(boolean failOnError) {
		this.failOnError = failOnError;
	}

	/**
     * Check that all required attributes have been set and nothing
     * silly has been entered.
     *
     * @since Ant 1.5
     * @exception BuildException if an error occurs
     */
    protected void checkParameters() throws BuildException {
		if (accessId == null)
			throw new BuildException("accessId must be set");
		if (secretKey == null)
			throw new BuildException("secretKey must be set");
    }
}
