package dak.ant.typedefs;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.DataType;
import org.apache.tools.ant.types.ResourceCollection;
import org.apache.tools.ant.types.selectors.AndSelector;
import org.apache.tools.ant.types.selectors.ContainsRegexpSelector;
import org.apache.tools.ant.types.selectors.ContainsSelector;
import org.apache.tools.ant.types.selectors.DateSelector;
import org.apache.tools.ant.types.selectors.DependSelector;
import org.apache.tools.ant.types.selectors.DepthSelector;
import org.apache.tools.ant.types.selectors.DifferentSelector;
import org.apache.tools.ant.types.selectors.ExtendSelector;
import org.apache.tools.ant.types.selectors.FileSelector;
import org.apache.tools.ant.types.selectors.FilenameSelector;
import org.apache.tools.ant.types.selectors.MajoritySelector;
import org.apache.tools.ant.types.selectors.NoneSelector;
import org.apache.tools.ant.types.selectors.NotSelector;
import org.apache.tools.ant.types.selectors.OrSelector;
import org.apache.tools.ant.types.selectors.PresentSelector;
import org.apache.tools.ant.types.selectors.SelectSelector;
import org.apache.tools.ant.types.selectors.SelectorContainer;
import org.apache.tools.ant.types.selectors.SizeSelector;
import org.apache.tools.ant.types.selectors.TypeSelector;
import org.apache.tools.ant.types.selectors.modifiedselector.ModifiedSelector;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class S3FileSet extends DataType implements ResourceCollection,SelectorContainer {
    private String accessId;
    private String secretKey;
    private String bucket;
    private String prefix;

    /**
     * Ant likes to be Java 1.2 compat. I don't. Other tasks in this package
     * assume Java 5, so I will :-)
     */
    private List<FileSelector> selectors = new ArrayList<FileSelector>();

    private List<File> selectedResources;

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public boolean isFilesystemOnly() {
        return false;
    }

    public AWSCredentials getCredentials() {
        return new AWSCredentials(accessId, secretKey);
    }

    public String getBucket() {
        return bucket;
    }

    @SuppressWarnings("unchecked")
    public Iterator iterator() {

        if (isReference()) {
            return ((S3FileSet) getCheckedRef(getProject())).iterator();
        }
        calculateSet();
        return selectedResources.iterator();
    }

    public int size() {

        if (isReference()) {
            return ((S3FileSet) getCheckedRef(getProject())).size();
        }
        calculateSet();
        return selectedResources.size();
    }

    private void calculateSet() {

        checkParameters();

        // not sure if we should cache...but I am

        if (selectedResources != null)
            return;

        selectedResources = new ArrayList<File>();

        AWSCredentials credentials = new AWSCredentials(accessId, secretKey);

        try {
            S3Service s3 = new RestS3Service(credentials);
            S3Bucket bucket = new S3Bucket(this.bucket);
            S3Object[] objectListing;

            if (prefix != null)
                objectListing = s3.listObjects(bucket, prefix, null);
            else
                objectListing = s3.listObjects(bucket);

            for (S3Object object : objectListing) {

                String fileName = object.getKey();
                S3File file = new S3File(fileName);
                file.setLastModified(object.getLastModifiedDate().getTime());

                if (isSelected(fileName, file)) {
                    selectedResources.add(file);
                }
            }
        } catch (Exception e) {
            throw new BuildException(e);
        }
    }

    protected boolean isSelected(String name, File file) {

        File basedir = new File("");

        for (int i = 0; i < selectors.size(); i++) {
            if (!((FileSelector) selectors.get(i)).isSelected(basedir, name,
                    file)) {
                return false;
            }
        }
        return true;
    }

    public synchronized void appendSelector(FileSelector selector) {
        if (isReference()) {
            throw noChildrenAllowed();
        }
        selectors.add(selector);
    }

    public void addSelector(SelectSelector selector) {
        appendSelector(selector);
    }

    public void addAnd(AndSelector selector) {
        appendSelector(selector);
    }

    public void addOr(OrSelector selector) {
        appendSelector(selector);
    }

    public void addNot(NotSelector selector) {
        appendSelector(selector);
    }

    public void addNone(NoneSelector selector) {
        appendSelector(selector);
    }

    public void addMajority(MajoritySelector selector) {
        appendSelector(selector);
    }

    public void addDate(DateSelector selector) {
        appendSelector(selector);
    }

    public void addSize(SizeSelector selector) {
        appendSelector(selector);
    }

    public void addFilename(FilenameSelector selector) {
        appendSelector(selector);
    }

    public void addType(TypeSelector selector) {
        appendSelector(selector);
    }

    public void addCustom(ExtendSelector selector) {
        appendSelector(selector);
    }

    public void addPresent(PresentSelector selector) {
        appendSelector(selector);
    }

    public void addDepth(DepthSelector selector) {
        appendSelector(selector);
    }

    public void addDepend(DependSelector selector) {
        appendSelector(selector);
    }

    public void addModified(ModifiedSelector selector) {
        appendSelector(selector);
    }

    public void add(FileSelector selector) {
        appendSelector(selector);
    }

    public void addContains(ContainsSelector selector) {
        // ignore, won't work
    }

    public void addContainsRegexp(ContainsRegexpSelector selector) {
        // ignore, won't work
    }

    public void addDifferent(DifferentSelector selector) {
        // ignore, won't work
    }

    public FileSelector[] getSelectors(Project aProject) {
        return (FileSelector[]) selectors.toArray();
    }

    public boolean hasSelectors() {
        return selectors.size() != 0;
    }

    public int selectorCount() {
        return selectors.size();
    }

    @SuppressWarnings("unchecked")
    public Enumeration selectorElements() {
        return Collections.enumeration(selectors);
    }

    protected void checkParameters() throws BuildException {
        if (accessId == null)
            throw new BuildException("accessId must be set");
        if (secretKey == null)
            throw new BuildException("secretKey must be set");
        if (bucket == null)
            throw new BuildException("bucket must be set");
    }

}
