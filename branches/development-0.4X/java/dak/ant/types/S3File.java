package dak.ant.types;

import java.io.File;

/**
 * Attempt to make S3 objects compatible with ant file selectors.
 * This could probably be done a lot better. One thing to explore would
 * be the URI constructor - this might allow bucket to be used as part
 * of the file path which might make some of the S3 operations easier
 * to handle? Or maybe harder!
 * 
 * It might be here that you could plug in a File System implementation that
 * uses any of a number of naming schemes to simulate directories within S3 -
 * a number of tools do this and it'd be nice to be able to read their
 * output. 
 * 
 * I should probably handle isAbsolute some how.
 * @author chris
 *
 */
public class S3File extends File
{
   private static final long serialVersionUID = 1825082919531083517L;

   private long lastModified;
   
   public S3File( String pathname )
   {
      super( pathname );
   }

   @Override
   public boolean isDirectory( )
   {
      return false; // currently no such thing as a directory.
   }
   
   @Override
   public boolean setLastModified( long time ) {
      lastModified = time;
      return true;
   }
   
   @Override
   public long lastModified( )
   {
      return lastModified;
   }
   
   
}
