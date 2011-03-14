package dak.ant.types;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.DataType;
import org.apache.tools.ant.types.PatternSet;
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
import org.jets3t.service.security.AWSCredentials;

import dak.ant.util.S3BucketScannerX;

public class S3FileSetX extends DataType implements ResourceCollection,SelectorContainer 
       { // INSTANCE VARIABLES
    
         private String             bucket;
         private String             prefix;
         private PatternSet         defaultPatterns      = new PatternSet();
         private List<PatternSet>   additionalPatterns   = new ArrayList<PatternSet>  ();
         private List<FileSelector> selectors            = new ArrayList<FileSelector>();
         private boolean            errorOnMissingBucket = true;
         
         private List<S3File>       selected;
         private S3BucketScannerX   scanner;

         // TASK ATTRIBUTES
    
         public void setBucket(String bucket) 
                { if (isReference())
                    throw tooManyAttributes();

                  this.bucket  = bucket;
                  this.scanner = null;
                }

         /** Returns the S3 bucket attribute, dereferencing it if required.
           * 
           */
         public String getBucket() 
                { if (isReference())
                     return getRef(getProject()).getBucket();

                  dieOnCircularReference();

                  return bucket;
                }
         
         public void setPrefix(String prefix) 
                { this.prefix = prefix;
                }

         /** Appends <code>includes</code> to the current list of include patterns.
           * <p>
           * Patterns may be separated by a comma or a space.
           * </p>
           * 
           * @param includes the <code>String</code> containing the include patterns.
           */
         public synchronized void setIncludes(String includes) 
                { if (isReference())
                     throw tooManyAttributes();

                  this.defaultPatterns.setIncludes(includes);
                  this.scanner = null;
                }

         /** Appends <code>excludes</code> to the current list of exclude patterns.
           * <p>
           * Patterns may be separated by a comma or a space.
           * </p>
           * 
           * @param excludes the <code>String</code> containing the exclude patterns.
           */
         public synchronized void setExcludes(String excludes) 
                { if (isReference())
                     throw tooManyAttributes();

                  defaultPatterns.setExcludes(excludes);
                  scanner = null;
                }

         /** Sets whether an error is thrown if a bucket does not exist. Default value
           * is <code>true</code>.
           * 
           * @param enabled <code>true</code> if missing buckets cause errors.
           */
         public void setErrorOnMissingBucket(boolean enabled) 
                { if (isReference())
                     throw tooManyAttributes();

                  this.errorOnMissingBucket = enabled;
                  this.scanner = null;
                }
         
         public boolean isFilesystemOnly() 
                { return false;
                }
         
         // SUPPORTED SELECTORS
         
         public void addDate(DateSelector selector) 
                { appendSelector(selector);
                }
         
         public synchronized void appendSelector(FileSelector selector) 
                { if (isReference()) 
                     { throw noChildrenAllowed();
                     }
                 
                  selectors.add(selector);
                }

         // IMPLEMENTATION

         @Override
         public Iterator<S3File> iterator()
                { return null;
                }

         @Override
         public int size()
                { return 0;
                }

         public Iterator<S3File> iterator(AWSCredentials credentials) 
                { if (isReference()) 
                     return ((S3FileSetX) getCheckedRef(getProject())).iterator(credentials);

                  calculateSet(credentials);

                  return selected.iterator();
                }

         public int size(AWSCredentials credentials) 
                { if (isReference()) 
                     return ((S3FileSetX) getCheckedRef(getProject())).size();

                  calculateSet(credentials);
    
                  return selected.size();
                }

         /** Performs the check for circular references and returns the referenced
           * S3FileSet.
           * 
           */
         private S3FileSetX getRef(Project project) 
                 { return (S3FileSetX) getCheckedRef(project);
                 }

         private void calculateSet(AWSCredentials credentials) 
                 { checkParameters();

                   if (selected != null)
                      return;

                   selected = new ArrayList<S3File>();

                   try 
                      { S3Service  service = new RestS3Service(credentials);
//                        S3Object[] objects;
//
//                        if (prefix != null)
//                           objects = service.listObjects(bucket,prefix,null);
//                           else
//                           objects = service.listObjects(bucket);

                        S3BucketScannerX scanner = getBucketScanner(getProject(),service);
                        S3File[]         objects = scanner.getIncludedObjects();
                        
                        for (S3File object: objects) 
                            { if (isSelected(object.getKey(),object)) 
                                 { selected.add(object);
                                 }
                            }
                      } 
                   catch (Exception x) 
                      { x.printStackTrace();
                        throw new BuildException(x);
                      }
                 }

         private void checkParameters() throws BuildException 
                 { if (bucket == null)
                      throw new BuildException("Missing 'bucket' attribute");
                 }

         private boolean isSelected(String name,S3File file)
                 { File basedir = new File("");

                   for (int i=0; i<selectors.size(); i++)
                       { if (!((FileSelector) selectors.get(i)).isSelected(basedir,name,file)) 
                            { return false;
                            }
                       }
        
                   return true;
                 }
         
         /** Returns an initialised S3 bucket scanner needed to fetch/filter the S3
           * objects to process.
           * 
           */
         private S3BucketScannerX getBucketScanner(Project project,S3Service service) throws Exception 
                 { // ... reference objectset ?

                   if (isReference())
                      return getRef(project).getBucketScanner(project, service);

                   dieOnCircularReference();

                   // ... real objectset !

                   S3BucketScannerX bs = null;

                   synchronized (this) 
                      { if ((scanner != null) && (project == getProject()))
                           bs = scanner;
                           else
                           { if (bucket == null)
                                throw new BuildException("No bucket specified for '" + getDataTypeName() + "'");

                             S3Bucket[] buckets = service.listAllBuckets();
                             boolean    exists  = false;

                             for (S3Bucket item : buckets) 
                                 { if (bucket.equals(item.getName())) 
                                     { exists = true;
                                       break;
                                     }
                                 }

                             if (!exists && errorOnMissingBucket)
                                throw new BuildException("Bucket '" + bucket + "' does not exist");

                             bs = new S3BucketScannerX(service);

                             setupBucketScanner(bs, project);
                             bs.setErrorOnMissingBucket(errorOnMissingBucket);

                             scanner = (project == getProject()) ? bs : scanner;
                           }
                      }

                   bs.scan();

                   return bs;
                 }

         /** Initialises the specified S3 bucket scanner against the specified project.
           * 
           */
         private synchronized void setupBucketScanner(S3BucketScannerX bs,Project project) 
                 { if (bs == null)
                      throw new IllegalArgumentException("S3BucketScanner cannot be null");

                   bs.setBucket(bucket);

                   PatternSet ps = mergePatterns(project);

                   bs.setSelected(selected);
                   bs.setIncludes(ps.getIncludePatterns(project));
                   bs.setExcludes(ps.getExcludePatterns(project));
                 }
         

         /** Get the merged patterns for this objectset.
           * 
           */
         public synchronized PatternSet mergePatterns(Project project) 
                { PatternSet ps = (PatternSet) defaultPatterns.clone();

                  for (PatternSet item : additionalPatterns) 
                      { ps.append(item, project);
                      }

                  return ps;
                }
         
         
         // *** ### ***

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

    @SuppressWarnings({ "rawtypes" })
    public Enumeration selectorElements() {
        return Collections.enumeration(selectors);
    }
}
