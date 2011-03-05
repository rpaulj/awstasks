package dak.ant.taskdefs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.types.LogLevel;

import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.utils.FileComparer;
import org.jets3t.service.utils.FileComparerResults;

import dak.ant.typedefs.S3ObjectSet;
import dak.ant.typedefs.S3ObjectWrapper;
import dak.ant.util.S3BucketScanner;

/**
 * Wraps the JetS3t download functionality in an Ant task.
 * 
 * @author Tony Seebregts
 */
public class S3Download extends AWSTask { 
    
    // CONSTANTS

    // INSTANCE VARIABLES

    private String dir;
    private String prefix = "";
    private List<S3ObjectSet> objectlists = new ArrayList<S3ObjectSet>();

    private boolean downloadAll = false;
    private boolean downloadNew = false;
    private boolean downloadChanged = false;

    // PROPERTIES

    /**
     * Sets the directory to which to download files.
     * 
     * @param dir
     *            Download directory. A BuildException will be thrown by
     *            checkParameters() if this is <code>null</code> or
     *            <code>blank</code>.
     */
    public void setDir(String dir) {
        this.dir = dir;
    }

    /**
     * Sets the (optional) prefix with which to store the downloaded files.
     * 
     * @param prefix
     *            Defaults to "".
     */
    public void setPrefix(String prefix) {
        this.prefix = (prefix == null) ? "" : prefix.trim();
    }

    /**
     * Accepts a comma delimited set of download options:
     * <ul>
     * <li>all - downloads all objects that match the selection criteria
     * <li>new - downloads any objects that match the selection criteria that do
     * not already exist in the download directory
     * <li>changed - downloads any objects that match the selection criteria
     * that have changed compared to the existing existing copy in the download
     * directory
     * </ul>
     * 
     * @param options
     *            Comma separated list of download options. Defaults to 'all'.
     */
    public void setDownload(String options) {
        if (options == null)
            return;

        String[] tokens = options.split(",");

        for (String token : tokens) {
            String _token = token.trim();

            if ("all".equalsIgnoreCase(_token))
                downloadAll = true;
            else if ("new".equalsIgnoreCase(_token))
                downloadNew = true;
            else if ("changed".equalsIgnoreCase(_token))
                downloadChanged = true;
        }
    }

    public S3ObjectSet createObjectSet() {
        S3ObjectSet objectset = new S3ObjectSet();

        objectlists.add(objectset);

        return objectset;
    }

    public S3ObjectSet createS3ObjectSet(S3ObjectSet list) {
        S3ObjectSet objectset = new S3ObjectSet();

        objectlists.add(objectset);

        return objectset;
    }

    // IMPLEMENTATION

    /**
     * Checks that the AWS credentials have been set and warns if the file list
     * is empty.
     * 
     * @since Ant 1.5
     * @exception BuildException
     *                if an error occurs
     */
    protected void checkParameters() throws BuildException {
        super.checkParameters();

        if ((dir == null) || dir.matches("\\s*"))
            throw new BuildException("'dir' attribute must be set");

        if (objectlists.isEmpty()) {
            log("No object lists - nothing to do!", LogLevel.WARN.getLevel());
            return;
        }
    }

    // TODO: optimise to avoid scanning an S3 bucket twice when downloading
    // new/changed files.
    public void execute() throws BuildException {
        checkParameters();

        AWSCredentials credentials = new AWSCredentials(accessId, secretKey);

        try {
            RestS3Service s3 = new RestS3Service(credentials);
            File directory = new File(dir);

            // ... process object sets

            for (S3ObjectSet objectset : objectlists) {
                S3BucketScanner scanner = objectset.getBucketScanner(s3);

                try {
                    String bucket = scanner.getBucket();
                    List<S3ObjectWrapper> objects = new ArrayList<S3ObjectWrapper>();

                    objects.addAll(Arrays.asList(scanner.getIncludedObjects()));

                    // ... download all

                    if (downloadAll || (!downloadNew && !downloadChanged)) {
                        log("Downloading " + objectset.size(s3)
                                + " items from '" + objectset.getBucket()
                                + "' to '" + dir + "'");

                        for (S3ObjectWrapper object : objects) {
                            fetch(s3, bucket, directory, object.getKey());
                        }
                    } else { // .... download new/changed

                        FileComparer fc = FileComparer.getInstance();
                        Map<String, File> map = buildFileMap(new File(dir),
                                prefix);
                        Map<String, StorageObject> _objects = fc
                                .buildObjectMap(s3, bucket, "", false, null);
                        FileComparerResults rs = fc.buildDiscrepancyLists(map,
                                _objects, null);
                        List<S3ObjectWrapper> list = new ArrayList<S3ObjectWrapper>();

                        if (downloadNew) {
                            for (String key : rs.onlyOnServerKeys) {
                                S3ObjectWrapper object = new S3ObjectWrapper(
                                        (S3Object) _objects.get(key));

                                if (objects.contains(object))
                                    list.add(object);
                            }
                        }

                        if (downloadChanged) {
                            for (String key : rs.updatedOnServerKeys) {
                                S3ObjectWrapper object = new S3ObjectWrapper(
                                        (S3Object) _objects.get(key));

                                if (objects.contains(object))
                                    list.add(object);
                            }
                        }

                        log("Downloading " + list.size() + " items from '"
                                + objectset.getBucket() + "' to '" + dir + "'");

                        for (S3ObjectWrapper object : list) {
                            fetch(s3, bucket, directory, object.getKey());
                        }
                    }
                } catch (BuildException x) {
                    if (failOnError)
                        throw x;

                    log("Could not retrieve files from Amazon S3 ["
                            + x.getMessage() + "]", LogLevel.ERR.getLevel());
                }
            }
        } catch (BuildException x) {
            throw x;
        } catch (Exception x) {
            throw new BuildException(x);
        }
    }

    private void fetch(RestS3Service s3, String bucket, File dir, String key)
            throws Exception {
        File file = new File(dir, key);

        if (verbose) {
            log("Downloading [" + key + "][" + file + "]");
        }

        S3Object object = s3.getObject(bucket, key);

        if ("application/x-directory".equals(object.getContentType())) {
            file.mkdirs();
            return;
        }

        byte[] buffer = new byte[16384];
        InputStream in = null;
        OutputStream out = null;
        int N;

        try {
            file.getParentFile().mkdirs();

            in = object.getDataInputStream();
            out = new FileOutputStream(file);

            while ((N = in.read(buffer)) != -1) {
                out.write(buffer, 0, N);
            }
        } finally {
            close(in);
            close(out);
        }
    }

    /**
     * Builds a jets3t file map for a directory..
     * 
     * @param dir
     *            Download directory.
     * 
     * @throws IOException
     *             Thrown if the file system fails on File.getCanonicalPath.
     */
    private Map<String, File> buildFileMap(File dir, String prefix)
            throws IOException {
        return buildFileMap(dir, scan(dir).toArray(new File[0]), prefix);
    }

    /**
     * Recursively iterates through a directory tree to build a list of files.
     * 
     * @param root
     *            Root directory to synchronise.
     * 
     * @throws IOException
     *             Thrown if the file system fails on File.getCanonicalPath.
     */
    private List<File> scan(File directory) throws IOException { // ... iterate
                                                                 // through
                                                                 // directory
                                                                 // tree

        List<File> list = new ArrayList<File>();
        File[] files;

        if ((files = directory.listFiles()) != null) {
            for (File file : files) {
                list.add(file);

                if (file.isDirectory())
                    list.addAll(scan(file));
            }
        }

        return list;
    }
}
