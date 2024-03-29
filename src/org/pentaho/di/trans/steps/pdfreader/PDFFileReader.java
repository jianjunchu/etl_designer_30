/* Copyright (c) 2007 Pentaho Corporation.  All rights reserved. 
 * This software was developed by Pentaho Corporation and is provided under the terms 
 * of the GNU Lesser General Public License, Version 2.1. You may not use 
 * this file except in compliance with the license. If you need a copy of the license, 
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. The Original Code is Pentaho 
 * Data Integration.  The Initial Developer is Pentaho Corporation.
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS" 
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to 
 * the license for the specific language governing your rights and limitations.*/

package org.pentaho.di.trans.steps.pdfreader;

import java.awt.Dialog;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.fileinput.FileInputList;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.xgn.reader.PDFReader; 

/**
 * Read all sorts of text files, convert them to rows and writes these to one or more output streams.
 * 
 * @author Matt
 * @since 4-apr-2003
 */
public class PDFFileReader extends BaseStep implements StepInterface
{
	private static Class<?> PKG = PDFFileReaderMeta.class;

	private PDFFileReaderMeta meta;

    private PDFFileReaderData data;

    PDFReader pdfReader = null;
    
    public PDFFileReader(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
    {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }
	
	/**
	 * Build an empty row based on the meta-data...
	 * 
	 * @return
	 */

	private Object[] buildEmptyRow()
	{
        Object[] rowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
 
		 return rowData;
	}

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
    {
    	if(!meta.isFileField())
		{
    		if (data.filenr >= data.filessize)
  	        {
  	            setOutputDone();
  	            return false;
  	        }
		}else
		{
			if (data.filenr >= data.filessize)
  	        {
				// Grab one row from previous step ...
				data.readrow=getRow();
  	        }

			if (data.readrow==null)
  	        {
  	            setOutputDone();
  	            return false;
  	        }
			
	        if (first)
	        {
	            first = false;

				data.inputRowMeta = getInputRowMeta();
				data.outputRowMeta = data.inputRowMeta.clone();
		        meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

	            // Get total previous fields
	            data.totalpreviousfields=data.inputRowMeta.size();
	            
	        	// Check is filename field is provided
				if (Const.isEmpty(meta.getDynamicFilenameField()))
				{
					logError(BaseMessages.getString(PKG,"GetFileNames.Log.NoField"));
					throw new KettleException(BaseMessages.getString(PKG,"GetFileNames.Log.NoField"));
				}
				
	            
				// cache the position of the field			
				if (data.indexOfFilenameField<0)
				{
					data.indexOfFilenameField =data.inputRowMeta.indexOfValue(meta.getDynamicFilenameField());
					if (data.indexOfFilenameField<0)
					{
						// The field is unreachable !
						logError(BaseMessages.getString(PKG,"GetFileNames.Log.ErrorFindingField",meta.getDynamicFilenameField())); //$NON-NLS-1$ //$NON-NLS-2$
						throw new KettleException(BaseMessages.getString(PKG,"GetFileNames.Exception.CouldnotFindField",meta.getDynamicFilenameField())); //$NON-NLS-1$ //$NON-NLS-2$
					}
				}  
				
	        	// If wildcard field is specified, Check if field exists
				if (!Const.isEmpty(meta.getDynamicWildcardField()))
				{
					if (data.indexOfWildcardField<0)
					{
						data.indexOfWildcardField =data.inputRowMeta.indexOfValue(meta.getDynamicWildcardField());
						if (data.indexOfWildcardField<0)
						{
							// The field is unreachable !
							logError(BaseMessages.getString(PKG,"GetFileNames.Log.ErrorFindingField")+ "[" + meta.getDynamicWildcardField()+"]"); //$NON-NLS-1$ //$NON-NLS-2$
							throw new KettleException(BaseMessages.getString(PKG,"GetFileNames.Exception.CouldnotFindField",meta.getDynamicWildcardField())); //$NON-NLS-1$ //$NON-NLS-2$
						}
					}
				}

	        }
		}// end if first
    	incrementLinesInput();
        try
        {
        	Object[] outputRow = buildEmptyRow();
        	int outputIndex = 0;
			Object extraData[] = new Object[data.nrStepFields];
        	if(meta.isFileField())
        	{
    			if (data.filenr >= data.filessize)
    		    {
    				// Get value of dynamic filename field ...
    	    		String filename=getInputRowMeta().getString(data.readrow,data.indexOfFilenameField);
    	    		String wildcard="";
    	    		if(data.indexOfWildcardField>=0)
    	    			wildcard=getInputRowMeta().getString(data.readrow,data.indexOfWildcardField);
    	    		
    	    		String[] filesname={filename};
    		      	String[] filesmask={wildcard};
    		      	String[] filesrequired={"N"};
    		      	// Get files list
    		      	data.files = meta.getDynamicFileList(getTransMeta(), filesname, filesmask, filesrequired);
    		      	data.filessize=data.files.nrOfFiles();
    		      	data.filenr=0;
    		     }
        		
        		// Clone current input row
    			outputRow = data.readrow.clone();
        	}
        	if(data.filessize>0)
        	{
	        	data.file = data.files.getFile(data.filenr);
	        	
	        	//jason	        	
	        	if(!data.pageInitFlag)
	        	{
	        	    if(data.file.getType().equals(FileType.FILE) && data.file.getName().getExtension().equalsIgnoreCase("pdf"))
		        	{
	        	    	try{
	        	    		pdfReader = new PDFReader(data.file);
	        	    		if(log.isBasic() && data.file!=null) logBasic("init pdf file successfuly."+ data.file.getURL());
	        	    	}catch(Exception ex)
	        	    	{
	        	    		if(log.isBasic() && data.file!=null) logBasic("init pdf file failed."+ data.file.getURL());
	        	    		ex.printStackTrace();
	        	    	}
	        	    	if(pdfReader != null)
	        	    	{
	        	    		try{
	        	    			data.pagesize = pdfReader.getPageCount();
	        	    		}catch (Exception ex)
	        	    		{
	        	    			data.pagesize = -1;
	        	    		}
	        	    		
	        	    		data.pagenr=0;
	        	    		data.pageInitFlag = true;
	        	    	}
	        	    	else  // init pdfReaer failed, skip it
		        	    {
		        	    	data.pagesize = -1;
			        		data.pagenr=-1;
		        	    }
		        	}
	        	    else
	        	    {
	        	    	data.pagesize = -1;
		        		data.pagenr=-1;
	        	    }
	        	}
	        	//jason end
	        	
                if(meta.isAddResultFile())
                {
         			// Add this to the result file names...
         			ResultFile resultFile = new ResultFile(ResultFile.FILE_TYPE_GENERAL, data.file, getTransMeta().getName(), getStepname());
         			resultFile.setComment(BaseMessages.getString(PKG,"GetFileNames.Log.FileReadByStep"));
         			addResultFile(resultFile);
                }
            	
                // filename
        		extraData[outputIndex++]=KettleVFS.getFilename(data.file);

                // short_filename
        		extraData[outputIndex++]=data.file.getName().getBaseName();

                try
                {
    				 // Path
                	 extraData[outputIndex++]=KettleVFS.getFilename(data.file.getParent());

                	 // type
    				 extraData[outputIndex++]=data.file.getType().toString();
    				 
                     // exists
    				 extraData[outputIndex++]=Boolean.valueOf(data.file.exists());
                    
                     // ishidden
    				 extraData[outputIndex++]=Boolean.valueOf(data.file.isHidden());

                     // isreadable
    				 extraData[outputIndex++]=Boolean.valueOf(data.file.isReadable());
    				
                     // iswriteable
    				 extraData[outputIndex++]=Boolean.valueOf(data.file.isWriteable());

                     // lastmodifiedtime
    				 extraData[outputIndex++]=new Date( data.file.getContent().getLastModifiedTime() );

                     // size
                     Long size = null;
                     if (data.file.getType().equals(FileType.FILE))
                     {
                         size = new Long( data.file.getContent().getSize() );
                     }
   
   				 	 extraData[outputIndex++]=size;
   				 	 
   				 	
                }
                catch (IOException e)
                {
                    throw new KettleException(e);
                }

                 // extension
	 		  	 extraData[outputIndex++]=data.file.getName().getExtension();
   	
                 // uri	
				 extraData[outputIndex++]= data.file.getName().getURI();
   	
                 // rooturi	
				 extraData[outputIndex++]= data.file.getName().getRootURI();
  
		         // See if we need to add the row number to the row...  
		         if (meta.includeRowNumber() && !Const.isEmpty(meta.getRowNumberField()))
		         {
					  extraData[outputIndex++]= new Long(data.rownr);
		         }
		         //jason See if we need to add the content to the row...  
		         if (meta.includeContent())
		         {
		        	 if (data.pagenr >= 0 ){
		        		 	extraData[outputIndex++]= new Long(data.pagenr+1);
		        		 	
		        		 try{
		        			 extraData[outputIndex++]= extractCotent(data.pagenr+1);
		        		 }catch(Throwable t)
		        		 {
		        			 extraData[outputIndex++]= null;
		        			 log.logError("ERROR:", "file name="+data.filename);
		        		 }
		        		 
		        	  }
		        	 else{
		        		 extraData[outputIndex++]= 0l;
		        		 extraData[outputIndex++]= null;
		        	 }
		         }
		         //	jason end
		         
		         
		         data.rownr++;
		        // Add row data
		        outputRow = RowDataUtil.addRowData(outputRow,data.totalpreviousfields, extraData);
                // Send row
		        putRow(data.outputRowMeta, outputRow);
		        
	      		if (meta.getRowLimit()>0 && data.rownr>=meta.getRowLimit())  // limit has been reached: stop now.
	      		{
	   	           setOutputDone();
	   	           return false;
	      		}	      		
//	      		if( getLinesInput()>=PDFFileReaderMeta.RESTRICT_MAX_LINE && !meta.getVersionName().equals(PDFFileReaderMeta.VERSION_RELEASE)  )
//	      		{
//		   	           setOutputDone();
//		   	           return false;
//		      	}
	      		
            }
        }
        catch (Exception e)
        {
        	e.printStackTrace();
            throw new KettleStepException(e);
        }

        //jason
        //data.filenr++;
        if(data.pagenr+1<data.pagesize)
        	data.pagenr++;
        else
        	{
        	 data.filenr++;
        	 data.pageInitFlag=false;
        	 if(this.pdfReader!=null)
        		 this.pdfReader.close();
        	}
        //		jason
        if (checkFeedback(getLinesInput())) 	
        {
        	if(log.isBasic()) logBasic(BaseMessages.getString(PKG,"GetFileNames.Log.NrLine",""+getLinesInput()));
        }

        return true;
    }

    //extract file content(pdf,word,ppt,etc)
    private String extractCotent(int pageNo) throws FileSystemException, IOException {
    	String content = null;
    	if(pdfReader == null)
    	{
    		if(log.isBasic()) logBasic("pdfReader is null set page content to null. pageNo= "+pageNo);
    			return null;
    	}
    	else{
    		if(log.isBasic()) logBasic("pdfReader is not null, begin to extract one page content"+pageNo);
    		try{
    		content  = pdfReader.getOnePageContent(pageNo);
    		}catch(Exception e)
    		{
    			if(log.isBasic()) logBasic("error extract file content, page="+pageNo + "exception = "+e.getMessage());
    		}
    	}
    	return content;
	}

	private void handleMissingFiles() throws KettleException
    { 
        List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();

        if (nonExistantFiles.size() != 0)
        {
            String message = FileInputList.getRequiredFilesDescription(nonExistantFiles);
            logBasic("ERROR: Missing " + message);
            throw new KettleException("Following required files are missing: " + message);
        }

        List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
        if (nonAccessibleFiles.size() != 0)
        {
            String message = FileInputList.getRequiredFilesDescription(nonAccessibleFiles);
            logBasic("WARNING: Not accessible " + message);
            throw new KettleException("Following required files are not accessible: " + message);
        }
    }

    public boolean init(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (PDFFileReaderMeta) smi;
        data = (PDFFileReaderData) sdi;

        if (super.init(smi, sdi))
        {
        	
			try
			{
				 // Create the output row meta-data
	            data.outputRowMeta = new RowMeta();
	            meta.getFields(data.outputRowMeta, getStepname(), null, null, this); // get the metadata populated
	            data.nrStepFields=  data.outputRowMeta.size();
	            
				if(!meta.isFileField())
				{
	                data.files = meta.getFileList(getTransMeta());
	                data.filessize=data.files.nrOfFiles();
					handleMissingFiles();
				}else
					data.filessize=0;
		            
			}
			catch(Exception e)
			{
				logError("Error initializing step: "+e.toString());
				logError(Const.getStackTracker(e));
				return false;
			}
		
            data.rownr = 1L;
			data.filenr = 0;
			data.totalpreviousfields=0;
            
            return true;
          
        }
        return false;
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (PDFFileReaderMeta) smi;
        data = (PDFFileReaderData) sdi;
        if(data.file!=null)
        {
        	try{
        	    	data.file.close();
        	    	data.file=null;
        	}catch(Exception e){}
        	
        }
        super.dispose(smi, sdi); 
    }

    //
    // Run is were the action happens!
//    public void run()
//    {
//    	BaseStep.runStepThread(this, meta, data);
//    }
}
