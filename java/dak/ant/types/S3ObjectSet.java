package dak.ant.types;

import java.util.ArrayList;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.DataType;
import org.apache.tools.ant.types.PatternSet;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Bucket;

import dak.ant.util.S3BucketScanner;

/**
 * Fileset look-alike for S3 buckets. Copied unashamedly from FileSet.
 * 
 * @author Tony Seebregts
 */
public class S3ObjectSet extends DataType { 
    
    // INSTANCE VARIABLES

    private String bucket;
    private boolean errorOnMissingBucket = true;
    private List<S3ObjectWrapper> selected = new ArrayList<S3ObjectWrapper>();
    private PatternSet defaultPatterns = new PatternSet();
    private List<PatternSet> additionalPatterns = new ArrayList<PatternSet>();
    private S3BucketScanner scanner;

    public S3ObjectSet() {
    }

    // PROPERTIES

    /**
     * Sets the S3 bucket for the objectset.
     * 
     */
    public void setBucket(String bucket) {
        if (isReference())
            throw tooManyAttributes();

        this.bucket = bucket;
        this.scanner = null;
    }

    /**
     * Returns the S3 bucket attribute, dereferencing it if required.
     * 
     */
    public String getBucket() {
        if (isReference())
            return getRef(getProject()).getBucket();

        dieOnCircularReference();

        return bucket;
    }

    /**
     * create&lt;Type&gt; implementation for an included <code>object</code>.
     * 
     */
    public S3ObjectWrapper createObject() {
        if (isReference())
            throw noChildrenAllowed();

        S3ObjectWrapper object = new S3ObjectWrapper();

        selected.add(object);

        this.scanner = null;

        return object;
    }

    /**
     * create&lt;Type&gt; implementation for an included <code>S3Object</code>
     * (for use where the neater &lt;object&gt; type causes a type conflict).
     * 
     */
    public S3ObjectWrapper createS3Object() {
        if (isReference())
            throw noChildrenAllowed();

        S3ObjectWrapper object = new S3ObjectWrapper();

        selected.add(object);

        this.scanner = null;

        return object;
    }

    /**
     * Appends <code>includes</code> to the current list of include patterns.
     * 
     * <p>
     * Patterns may be separated by a comma or a space.
     * </p>
     * 
     * @param includes
     *            the <code>String</code> containing the include patterns.
     */
    public synchronized void setIncludes(String includes) {
        if (isReference())
            throw tooManyAttributes();

        this.defaultPatterns.setIncludes(includes);
        this.scanner = null;
    }

    /**
     * Appends <code>excludes</code> to the current list of exclude patterns.
     * 
     * <p>
     * Patterns may be separated by a comma or a space.
     * </p>
     * 
     * @param excludes
     *            the <code>String</code> containing the exclude patterns.
     */
    public synchronized void setExcludes(String excludes) {
        if (isReference())
            throw tooManyAttributes();

        defaultPatterns.setExcludes(excludes);
        scanner = null;
    }

    /**
     * Sets whether an error is thrown if a bucket does not exist. Default value
     * is <code>true</code>.
     * 
     * @param enabled
     *            <code>true</code> if missing buckets cause errors.
     */
    public void setErrorOnMissingBucket(boolean enabled) {
        if (isReference())
            throw tooManyAttributes();

        this.errorOnMissingBucket = enabled;
        this.scanner = null;
    }

    /**
     * Scans the S3 bucket and returns a count of the number of S3 objects that
     * match the selection criteria.
     * 
     */
    public int size(S3Service service) throws Exception {
        if (isReference())
            return getRef(getProject()).size(service);

        dieOnCircularReference();

        return getBucketScanner(getProject(), service)
                .getIncludedObjectsCount();
    }

    // INSTANCE METHODS

    /**
     * Returns the S3 bucket scanner needed to fetch/filter the S3 objects to
     * process.
     * 
     * @throws Exception
     * 
     */
    public S3BucketScanner getBucketScanner(S3Service service) throws Exception {
        if (isReference())
            return getRef(getProject()).getBucketScanner(service);

        dieOnCircularReference();

        return getBucketScanner(getProject(), service);
    }

    /**
     * Returns an initialised S3 bucket scanner needed to fetch/filter the S3
     * objects to process.
     * 
     */
    private S3BucketScanner getBucketScanner(Project project, S3Service service)
            throws Exception { // ... reference objectset ?

        if (isReference())
            return getRef(project).getBucketScanner(project, service);

        dieOnCircularReference();

        // ... real objectset !

        S3BucketScanner bs = null;

        synchronized (this) {
            if ((scanner != null) && (project == getProject()))
                bs = scanner;
            else {
                if (bucket == null)
                    throw new BuildException("No bucket specified for '"
                            + getDataTypeName() + "'");

                S3Bucket[] buckets = service.listAllBuckets();
                boolean exists = false;

                for (S3Bucket item : buckets) {
                    if (bucket.equals(item.getName())) {
                        exists = true;
                        break;
                    }
                }

                if (!exists && errorOnMissingBucket)
                    throw new BuildException("Bucket '" + bucket
                            + "' does not exist");

                bs = new S3BucketScanner(service);

                setupBucketScanner(bs, project);
                bs.setErrorOnMissingBucket(errorOnMissingBucket);

                scanner = (project == getProject()) ? bs : scanner;
            }
        }

        bs.scan();

        return bs;
    }

    /**
     * Performs the check for circular references and returns the referenced
     * S3ObjectSet.
     * 
     */
    private S3ObjectSet getRef(Project project) {
        return (S3ObjectSet) getCheckedRef(project);
    }

    /**
     * Initialises the specified S3 bucket scanner against the specified
     * project.
     * 
     */
    private synchronized void setupBucketScanner(S3BucketScanner bs,
            Project project) {
        if (isReference()) {
            getRef(project).setupBucketScanner(bs, project);
            return;
        }

        dieOnCircularReference(project);

        if (bs == null)
            throw new IllegalArgumentException("S3BucketScanner cannot be null");

        bs.setBucket(bucket);

        PatternSet ps = mergePatterns(project);

        bs.setSelected(selected);
        bs.setIncludes(ps.getIncludePatterns(project));
        bs.setExcludes(ps.getExcludePatterns(project));
    }

    /**
     * Get the merged patterns for this objectset.
     * 
     */
    public synchronized PatternSet mergePatterns(Project project) {
        if (isReference()) {
            return getRef(project).mergePatterns(project);
        }

        dieOnCircularReference();

        PatternSet ps = (PatternSet) defaultPatterns.clone();

        for (PatternSet item : additionalPatterns) {
            ps.append(item, project);
        }

        return ps;
    }
}