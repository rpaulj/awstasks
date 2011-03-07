package dak.ant.util;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.types.selectors.SelectorUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Object;

import dak.ant.types.S3ObjectWrapper;

/**
 * S3 analogue to Ant FileSet DirectoryScanner. Copied unashamedly from
 * DirectoryScanner and then simplified as much as possible.
 * 
 * @author Tony Seebregts
 * 
 */
public class S3BucketScanner { 
    
    // INSTANCE VARIABLES

    private final S3Service service;

    private String bucket;
    private boolean errorOnMissingBucket = true;
    private String[] selected = new String[0];
    private String[] includes;
    private String[] excludes;

    private ConcurrentSkipListSet<S3ObjectWrapper> included = new ConcurrentSkipListSet<S3ObjectWrapper>();

    // CONSTRUCTOR

    /**
     * Initialises the internal S3 service.
     * 
     * @param service
     *            Initialised S3 service.
     */
    public S3BucketScanner(S3Service service) { // ... validate

        if (service == null)
            throw new IllegalArgumentException("Invalid S3 service");

        // ... initialise

        this.service = service;
    }

    // PROPERTIES

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getBucket() {
        return bucket;
    }

    /**
     * Sets the list of explicitly selected objects.
     * 
     */
    public void setSelected(List<S3ObjectWrapper> selected) {
        String[] keys = new String[selected.size()];
        int index = 0;

        for (S3ObjectWrapper item : selected)
            keys[index++] = item.getKey();

        this.selected = keys;
    }

    /**
     * Set the list of include patterns to use. All '\' characters are replaced
     * by <code>/</code> to match the S3 convention.
     * <p>
     * When a pattern ends with a '/' or '\', "**" is appended.
     * 
     * @param includes
     *            A list of include patterns. May be <code>null</code>,
     *            indicating that all objects should be included. If a non-
     *            <code>null</code> list is given, all elements must be non-
     *            <code>null</code>.
     */
    public synchronized void setIncludes(String[] includes) {
        if (includes == null) {
            this.includes = null;
        } else {
            this.includes = new String[includes.length];

            for (int i = 0; i < includes.length; i++) {
                this.includes[i] = normalizePattern(includes[i]);
            }
        }
    }

    /**
     * Set the list of include patterns to use. All '\' characters are replaced
     * by <code>/</code> to match the S3 convention.
     * <p>
     * When a pattern ends with a '/' or '\', "**" is appended.
     * 
     * @param excludes
     *            A list of exclude patterns. May be <code>null</code>,
     *            indicating that no files should be excluded. If a non-
     *            <code>null</code> list is given, all elements must be non-
     *            <code>null</code>.
     */
    public synchronized void setExcludes(String[] excludes) {
        if (excludes == null) {
            this.excludes = null;
        } else {
            this.excludes = new String[excludes.length];

            for (int i = 0; i < excludes.length; i++) {
                this.excludes[i] = normalizePattern(excludes[i]);
            }
        }
    }

    public void setErrorOnMissingBucket(boolean enabled) {
        this.errorOnMissingBucket = enabled;
    }

    // IMPLEMENTATION

    public S3ObjectWrapper[] getIncludedObjects() {
        return included.toArray(new S3ObjectWrapper[0]);
    }

    public int getIncludedObjectsCount() {
        return included.size();
    }

    public synchronized void scan() {
        try { // ... validate

            if (service.getBucket(bucket) == null)
                if (errorOnMissingBucket)
                    throw new BuildException("S3 bucket '" + bucket
                            + "' does not exist");

            // ... set include/exclude lists

            if (selected == null)
                selected = new String[0];

            if (includes == null)
                includes = (selected.length == 0) ? new String[] { SelectorUtils.DEEP_TREE_MATCH }
                        : new String[0];

            if (excludes == null)
                excludes = new String[0];

            // ... clear cached results

            included = new ConcurrentSkipListSet<S3ObjectWrapper>();

            // ... scan object list

            S3Object[] list = service.listObjects(bucket);

            for (S3Object item : list) {
                String key = item.getKey();
                boolean selected = false;
                boolean included = false;
                boolean excluded = false;

                for (String pattern : this.selected) {
                    if (SelectorUtils.match(pattern, key))
                        selected = true;
                }

                for (String pattern : includes) {
                    if (SelectorUtils.match(pattern, key))
                        included = true;
                }

                for (String pattern : excludes) {
                    if (SelectorUtils.match(pattern, key))
                        excluded = true;
                }

                if (selected || (included && !excluded))
                    this.included.add(new S3ObjectWrapper(item));
            }
        } catch (BuildException x) {
            throw x;
        } catch (Exception x) {
            throw new BuildException(x);
        }
    }

    /**
     * All '/' and '\' characters are replaced by <code>/</code> to match the S3
     * storage convention.
     * 
     * <p>
     * When a pattern ends with a '/' or '\', "**" is appended.
     * 
     */
    private static String normalizePattern(String p) {
        String pattern = p.replace('\\', '/');

        if (pattern.endsWith("/")) {
            pattern += SelectorUtils.DEEP_TREE_MATCH;
        }

        return pattern;
    }
}
