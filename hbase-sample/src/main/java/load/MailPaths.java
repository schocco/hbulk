package load;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;


// TODO: Auto-generated Javadoc
/**
 * The Class MailPaths.
 */
public class MailPaths {
	
	/**
	 * Gets all folder paths, separated by comma as a string.
	 *
	 * @param rootDirectory the root directory
	 * @return the string
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	static String get(String rootDirectory) throws IOException{

	 FileVisitor<Path> fileProcessor = new ProcessFile();
	    Files.walkFileTree(Paths.get(rootDirectory), fileProcessor);
	    return((ProcessFile) fileProcessor).getPathStr();
	}
	
	
	/**
	 * A FileVisitor that collects paths to files in directories named {@value #ALL_DOCUMENTS}.
	 * Other folders are skipped.
	 */
	private static final class ProcessFile extends SimpleFileVisitor<Path> {
		
		/** The Constant ALL_DOCUMENTS. */
		private static final String ALL_DOCUMENTS = "all_documents";
		
		/** The path str. */
		private StringBuffer pathStr = new StringBuffer(5000);
			    
	    /* (non-Javadoc)
    	 * @see java.nio.file.SimpleFileVisitor#postVisitDirectory(java.lang.Object, java.io.IOException)
    	 */
    	@Override
	    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
	    	if(dir.getFileName().toString().equals(ALL_DOCUMENTS)){
	    		if(pathStr.length() > 0){
	    			pathStr.append(",");
	    		}
	    		pathStr.append(dir.toString());
	    		return FileVisitResult.SKIP_SIBLINGS;
	    	} else {
		    	return super.postVisitDirectory(dir, exc);
	    	}

	    }
	    
	    /**
    	 * Gets the path string.
    	 *
    	 * @return the path string
    	 */
    	public String getPathStr(){
	    	return pathStr.toString();
	    }

	  }

}
