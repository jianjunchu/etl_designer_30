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

package org.pentaho.di.job.entries.excelutil;

import static org.pentaho.di.job.entry.validator.AndValidator.putValidators;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.andValidator;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.notBlankValidator;

import java.io.*;
import java.util.List;


import org.apache.commons.vfs.FileObject;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogWriter;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.resource.ResourceReference;
import org.w3c.dom.Node;

/**
 * This defines an Excel job entry.
 * 
 * @author Jason
 * @since 20090601
 * 
 */
public class JobEntryExcelUtil extends JobEntryBase implements Cloneable,
		JobEntryInterface {
	private String targetFilename;
	private String[] sourceFileName;
	private boolean addToResult;
	
	public JobEntryExcelUtil(String n) {
		super(n, "");
		setID(-1L);
	}

	public JobEntryExcelUtil() {
		this("");
	}

//	public JobEntryExcelUtil(JobEntryBase jeb) {
//		super(jeb);
//	}

	public Object clone() {
		JobEntryExcelUtil je = (JobEntryExcelUtil) super.clone();
		return je;
	}

	public String getXML() {
		StringBuffer retval = new StringBuffer(300);

		retval.append(super.getXML());
		retval.append("      ").append(
				XMLHandler.addTagValue("targetfile", targetFilename));

		retval.append("    <sourcefile>").append(Const.CR);
		for (int i = 0; i < this.getSourceFileName().length; i++) {
			retval.append("      <element>").append(Const.CR);
			retval.append("        ").append(
					XMLHandler.addTagValue("path", getSourceFileName()[i]));
			retval.append("      </element>").append(Const.CR);

		}
		retval.append("    </sourcefile>").append(Const.CR);
		
		retval.append("      ").append(
				XMLHandler.addTagValue("addtoresult", addToResult));

		return retval.toString();
	}

	public void loadXML(Node entrynode, List<DatabaseMeta> databases,
			List<SlaveServer> slaveServers, Repository rep)
			throws KettleXMLException {
		try {
			super.loadXML(entrynode, databases, slaveServers);
			targetFilename = XMLHandler.getTagValue(entrynode, "targetfile");
			Node appendElements = XMLHandler
					.getSubNode(entrynode, "sourcefile");
			int nrelements = XMLHandler.countNodes(appendElements, "element");
			allocate(nrelements);
			for (int i = 0; i < nrelements; i++) {
				Node fnode = XMLHandler.getSubNodeByNr(appendElements,
						"element", i);
				sourceFileName[i] = XMLHandler.getTagValue(fnode, "path");
			}			

			setAddToResult("Y".equalsIgnoreCase(XMLHandler.getTagValue(
					entrynode, "addtoresult")));

		} catch (KettleXMLException xe) {
			throw new KettleXMLException(
					"Unable to load job entry of type 'HTTP' from XML node", xe);
		}
	}

	public void loadRep(Repository rep, ObjectId id_jobentry,
			List<DatabaseMeta> databases, List<SlaveServer> slaveServers)
			throws KettleException {
		try {
			super.loadRep(rep, id_jobentry, databases, slaveServers);
			targetFilename = rep.getJobEntryAttributeString(id_jobentry,
					"targetfilename");
			int nrappendElement = rep.countNrJobEntryAttributes(id_jobentry,
					"source_path");
			allocate(nrappendElement);
			for (int i = 0; i < nrappendElement; i++) {
				sourceFileName[i] = rep.getJobEntryAttributeString(id_jobentry, i,
						"source_path");
			}

			addToResult = rep.getJobEntryAttributeBoolean(id_jobentry,
					"add_to_result");
			
		} catch (KettleException dbe) {
			throw new KettleException(
					"Unable to load job entry of type 'HTTP' from the repository for id_jobentry="
							+ id_jobentry, dbe);
		}
	}

	public void saveRep(Repository rep, ObjectId id_job) throws KettleException {
		try {
			super.saveRep(rep, id_job);

			rep.saveJobEntryAttribute(id_job, getObjectId(), "targetfilename",
					targetFilename);
			rep.saveJobEntryAttribute(id_job, getObjectId(), "add_to_result",
					addToResult);
			if (getSourceFileName() != null)
			for (int i = 0; i < getSourceFileName().length; i++) {
				rep.saveJobEntryAttribute(id_job, getObjectId(), i, "source_path",
						getSourceFileName()[i]);
			}
		} catch (KettleDatabaseException dbe) {
			throw new KettleException(
					"Unable to load job entry of type 'HTTP' to the repository for id_job="
							+ id_job, dbe);
		}
	}

	/**
	 * @return Returns the target filename.
	 */
	public String getTargetFilename() {
		return targetFilename;
	}

	/**
	 * @param targetFilename
	 *            The target filename to set.
	 */
	public void setTargetFilename(String targetFilename) {
		this.targetFilename = targetFilename;
	}

	/**
	 * We made this one synchronized in the JVM because otherwise, this is not
	 * thread safe. In that case if (on an application server for example)
	 * several HTTP's are running at the same time, you get into problems
	 * because the System.setProperty() calls are system wide!
	 */
	public synchronized Result execute(Result previousResult, int nr) {

		LogWriter log = LogWriter.getInstance();

		Result result = previousResult;
		result.setResult(false);

		logBasic(toString(), "Start of EXCEL Util job entry.");
		String outputFileName = null;
		
		if ( getSourceFileName()== null || getTargetFilename()== null )
		{
			result.setNrErrors(1);
	        logBasic(getName(), "no Source File or Target File");
		}
		else
		{
			outputFileName = environmentSubstitute(this.getTargetFilename());
		try {
			mergeExcels (this.getSourceFileName(),outputFileName);
		}
		 catch (Exception e) {
			e.printStackTrace();
			result.setNrErrors(1);
	        logError(getName(), "can not merge excels to a file because of a IOException: "
	            + e.getMessage());
	        logError(toString(), Const.getStackTracker(e));
		}
		}
		if (this.isAddToResult())
		{
			FileObject targetFile;
			try {
				targetFile = KettleVFS.getFileObject(outputFileName);
				ResultFile resultFile = new ResultFile(
						ResultFile.FILE_TYPE_GENERAL, targetFile, parentJob
								.getJobname(), toString());
				resultFile.setComment(""); //$NON-NLS-1$
				result.getResultFiles().put(resultFile.getFile().toString(),
						resultFile);
			} catch (Exception e) {
				result.setNrErrors(1);
				logError(getName(), "can not add target file to result files because of a IOException: "
		            + e.getMessage());
		        logError(toString(), Const.getStackTracker(e));
			}
			
		}
		result.setResult(true);

		// Get previous result rows...
		//List<RowMetaAndData> resultRows;

		return result;
	}

	public void mergeExcels(String[] inputFileName, String outputFile)
			throws Exception {
		LogWriter log = LogWriter.getInstance();
		String extention = outputFile.substring(outputFile.indexOf(".")+1,outputFile.length());
		XSSFWorkbook outputWorkbook = new XSSFWorkbook() ;
		for (int i = 0; i < inputFileName.length; i++) {
			File srcFile = new File(inputFileName[i]);
			if(!srcFile.exists())
			{
				logBasic(toString(), inputFileName[i]+" doesn't exist");
				continue;
			}
			InputStream is = new FileInputStream(srcFile);
			XSSFWorkbook srcWorkbook= new XSSFWorkbook(is);
			//Workbook srcWorkbook = Workbook.getWorkbook(srcFile);
			int number = srcWorkbook.getNumberOfSheets();
			//Sheet[] sheets = srcWorkbook.getNumberOfSheets();

			for (int j = 0; j < number; j++) {
				XSSFSheet sourSheet = srcWorkbook.getSheetAt(j);
				String sheetName = sourSheet.getSheetName();
				//XSSFSheet destSheet =outputWorkbook.createSheet("sheet"+i+"_"+j);
				String outputSheetName = sheetName;
				int index=1;
				while (outputWorkbook.getSheet(outputSheetName)!=null)
					outputSheetName = sheetName+"_"+index;
				XSSFSheet destSheet =outputWorkbook.createSheet(outputSheetName);
				WriteUtils.copySheet(sourSheet,destSheet);
			}
		}

		try {
			//OutputStream os = new FileOutputStream(outputFile);
			OutputStream os = KettleVFS.getOutputStream(outputFile, false);
			outputWorkbook.write(os);
			os.flush();
			os.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (outputWorkbook != null)
				outputWorkbook.close();
		}
	}

	public boolean evaluates() {
		return true;
	}

	public List<ResourceReference> getResourceDependencies(JobMeta jobMeta) {
		List<ResourceReference> references = super
				.getResourceDependencies(jobMeta);
		return references;
	}

	@Override
	public void check(List<CheckResultInterface> remarks, JobMeta jobMeta) {
		andValidator().validate(this,
				"targetFilename", remarks, putValidators(notBlankValidator())); //$NON-NLS-1$
	}

	public String[] getSourceFileName() {
		return sourceFileName;
	}

	public void setSourceFileName(String[] sourceFileName) {
		this.sourceFileName = sourceFileName;
	}

	public void allocate(int nrElements) {
		sourceFileName = new String[nrElements];
	}

	public boolean isAddToResult() {
		return addToResult;
	}

	public void setAddToResult(boolean addToResult) {
		this.addToResult = addToResult;
	}
}
