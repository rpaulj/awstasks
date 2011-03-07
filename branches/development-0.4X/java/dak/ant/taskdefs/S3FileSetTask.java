package dak.ant.taskdefs;

import java.util.ArrayList;
import java.util.List;

import dak.ant.types.S3FileSet;

public abstract class S3FileSetTask extends AWSTask {

   protected List<S3FileSet> fileSets = new ArrayList<S3FileSet>();

   public void addS3FileSet(S3FileSet set) {
       fileSets.add( set );
   }
   
   protected boolean isFileSetMode() {
      return fileSets.size( ) > 0;
   }

}
