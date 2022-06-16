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

 package com.xgn.di.trans.steps.ganji;

 import java.util.List;
 import java.util.Map;

 import org.eclipse.swt.widgets.Shell;
 import org.pentaho.di.core.CheckResult;
 import org.pentaho.di.core.CheckResultInterface;
 import org.pentaho.di.core.Const;
 import org.pentaho.di.core.Counter;
 import org.pentaho.di.core.database.DatabaseMeta;
 import org.pentaho.di.core.exception.KettleException;
 import org.pentaho.di.core.exception.KettleStepException;
 import org.pentaho.di.core.exception.KettleXMLException;
 import org.pentaho.di.core.fileinput.FileInputList;
 import org.pentaho.di.core.row.RowMetaInterface;
 import org.pentaho.di.core.row.ValueMeta;
 import org.pentaho.di.core.row.ValueMetaInterface;
 import org.pentaho.di.core.variables.VariableSpace;
 import org.pentaho.di.core.xml.XMLHandler;
 import org.pentaho.di.repository.Repository;
 import org.pentaho.di.trans.Trans;
 import org.pentaho.di.trans.TransMeta;
 import org.pentaho.di.trans.step.BaseStepMeta;
 import org.pentaho.di.trans.step.StepDataInterface;
 import org.pentaho.di.trans.step.StepDialogInterface;
 import org.pentaho.di.trans.step.StepInterface;
 import org.pentaho.di.trans.step.StepMeta;
 import org.pentaho.di.trans.step.StepMetaInterface;
 import org.pentaho.di.repository.ObjectId;
 import com.xgn.di.trans.steps.ganji.Messages;
 import org.w3c.dom.Node;




 /**
  * Ganji web site info extractor
  *
  * @author Jason
  * @since 10-Aug-2010
  */

 public class GanjiMeta extends BaseStepMeta implements StepMetaInterface
 {
	 public static final String VERSION_FREE = "????";
	 public static final String VERSION_COMMERCIAL = "?????";
	 public static final int RESTRICT_MAX_LINE = 10;
	 String version =VERSION_COMMERCIAL;
	 private long rowLimit;
	 private String[] urls;
	 private String[] listPageCount;
	 private String[] required;
	 private boolean includeRowNumber;
	 private String rowNumberField;

	 private String[] keyNames;
	 private String[] fieldNames;

	 public GanjiMeta()
	 {
		 super(); // allocate BaseStepMeta
	 }

	 public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
			 throws KettleXMLException
	 {
		 readData(stepnode);
	 }

	 public Object clone()
	 {
		 Object retval = super.clone();
		 return retval;
	 }

	 private void readData(Node stepnode) throws KettleXMLException
	 {
		 try
		 {
			 includeRowNumber  = "Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "rownum"));
			 rowNumberField    = XMLHandler.getTagValue(stepnode, "rownum_field");
			 rowLimit = Const.toLong(XMLHandler.getTagValue(stepnode, "limit"), 0L);

			 Node keynode = XMLHandler.getSubNode(stepnode, "keys");
			 int nrkeys   = XMLHandler.countNodes(keynode, "key_name");

			 Node filenode = XMLHandler.getSubNode(stepnode, "urls");
			 int nrfiles   = XMLHandler.countNodes(filenode, "url");
			 allocate(nrfiles,nrkeys);
			 for (int i = 0; i < nrfiles; i++)
			 {
				 Node urlsnode     = XMLHandler.getSubNodeByNr(filenode, "url", i);
				 Node listPageCountnode     = XMLHandler.getSubNodeByNr(filenode, "list_page_count", i);
				 Node requirednode = XMLHandler.getSubNodeByNr(filenode, "required", i);
				 urls[i]           = XMLHandler.getNodeValue(urlsnode);
				 listPageCount[i]  = XMLHandler.getNodeValue(listPageCountnode);
				 required[i]       = XMLHandler.getNodeValue(requirednode);
			 }

			 for (int i = 0; i < nrkeys; i++)
			 {
				 Node keyNamenode     = XMLHandler.getSubNodeByNr(keynode, "key_name", i);
				 Node fieldNamenode = XMLHandler.getSubNodeByNr(keynode, "field_name", i);
				 keyNames[i]  = XMLHandler.getNodeValue(keyNamenode);
				 fieldNames[i]       = XMLHandler.getNodeValue(fieldNamenode);
			 }
		 }
		 catch (Exception e)
		 {
			 throw new KettleXMLException("Unable to load step info from XML", e);
		 }
	 }


	 public String getXML()
	 {
		 StringBuffer retval = new StringBuffer(300);

		 retval.append("    ").append(XMLHandler.addTagValue("rownum",          includeRowNumber));
		 retval.append("    ").append(XMLHandler.addTagValue("rownum_field",    rowNumberField));
		 retval.append("    ").append(XMLHandler.addTagValue("limit", rowLimit));

		 retval.append("    <urls>").append(Const.CR);
		 for (int i = 0; i < urls.length; i++)
		 {
			 retval.append("      ").append(XMLHandler.addTagValue("url", urls[i]));
			 retval.append("      ").append(XMLHandler.addTagValue("list_page_count", listPageCount[i]));
			 retval.append("      ").append(XMLHandler.addTagValue("required", required[i]));
		 }
		 retval.append("    </urls>").append(Const.CR);

		 retval.append("    <keys>").append(Const.CR);
		 for (int i = 0; i < keyNames.length; i++)
		 {
			 retval.append("      ").append(XMLHandler.addTagValue("key_name", keyNames[i]));
			 retval.append("      ").append(XMLHandler.addTagValue("field_name", fieldNames[i]));
		 }
		 retval.append("    </keys>").append(Const.CR);

		 return retval.toString();
	 }

	 public void setDefault()
	 {
	 }

	 public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
			 throws KettleException
	 {
		 try
		 {
///			int nrfiles = rep.countNrStepAttributes(id_step, "url");
//			int nrkeys = rep.countNrStepAttributes(id_step, "key_name");

//			includeRowNumber  = rep.getStepAttributeBoolean(id_step, "rownum");
//			rowNumberField    = rep.getStepAttributeString (id_step, "rownum_field");
//			rowLimit          = rep.getStepAttributeInteger(id_step, "limit");

//			allocate(nrfiles,nrkeys);

//			for (int i = 0; i < nrfiles; i++)
//			{
//				urls[i] = rep.getStepAttributeString(id_step, i, "url");
//				listPageCount[i] = rep.getStepAttributeString(id_step, i, "page_count");
//				required[i] = rep.getStepAttributeString(id_step, i, "required");
//				if(!Messages.getString("System.Combo.Yes").equalsIgnoreCase(required[i])) required[i] = Messages.getString("System.Combo.No");
//			}
//			for (int i = 0; i < nrkeys; i++)
//			{
//				this.keyNames[i] = rep.getStepAttributeString(id_step, i, "key_name");
//				this.fieldNames[i] = rep.getStepAttributeString(id_step, i, "field_name");
//			}

		 }
		 catch (Exception e)
		 {
			 throw new KettleException("Unexpected error reading step information from the repository", e);
		 }
	 }

	 public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step)
			 throws KettleException
	 {
		 try
		 {
			 //		rep.saveStepAttribute(id_transformation, id_step, "rownum",          includeRowNumber);
			 //		rep.saveStepAttribute(id_transformation, id_step, "rownum_field",    rowNumberField);
			 //		rep.saveStepAttribute(id_transformation, id_step, "limit",           rowLimit);
			 //if(urls!=null && urls.length>0)
			 //{
			 //	for (int i = 0; i < urls.length; i++)
			 //	{
			 //		rep.saveStepAttribute(id_transformation, id_step, i, "url", urls[i]);
			 //		rep.saveStepAttribute(id_transformation, id_step, i, "page_count", listPageCount[i]);
			 //		rep.saveStepAttribute(id_transformation, id_step, i, "required", required[i]);
			 //	}
			 //}
			 //if(keyNames!=null && keyNames.length>0)
			 //{
			 //	for (int i = 0; i < keyNames.length; i++)
			 //	{
			 //		rep.saveStepAttribute(id_transformation, id_step, i, "key_name", keyNames[i]);
			 //		rep.saveStepAttribute(id_transformation, id_step, i, "field_name", this.fieldNames[i]);
			 //	}
			 //}

		 }
		 catch (Exception e)
		 {
			 throw new KettleException("Unable to save step information to the repository for id_step=" + id_step, e);
		 }
	 }

	 public void getFields(RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) throws KettleStepException
	 {
		 for(int i=0;i<this.getFieldNames().length;i++)
		 {
			 ValueMetaInterface filename = new ValueMeta(this.getFieldNames()[i],ValueMeta.TYPE_STRING);
			 filename.setLength(1000);
			 filename.setPrecision(-1);
			 filename.setOrigin(name);
			 row.addValueMeta(filename);
		 }

	 }

	 public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepinfo, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
	 {
		 CheckResult cr;
		 if (prev==null || prev.size()==0)
		 {
			 cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, Messages.getString("DummyTransMeta.CheckResult.NotReceivingFields"), stepinfo); //$NON-NLS-1$
			 remarks.add(cr);
		 }
		 else
		 {
			 cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, Messages.getString("DummyTransMeta.CheckResult.StepRecevingData",prev.size()+""), stepinfo); //$NON-NLS-1$ //$NON-NLS-2$
			 remarks.add(cr);
		 }

		 // See if we have input streams leading to this step!
		 if (input.length>0)
		 {
			 cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, Messages.getString("DummyTransMeta.CheckResult.StepRecevingData2"), stepinfo); //$NON-NLS-1$
			 remarks.add(cr);
		 }
		 else
		 {
			 cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, Messages.getString("DummyTransMeta.CheckResult.NoInputReceivedFromOtherSteps"), stepinfo); //$NON-NLS-1$
			 remarks.add(cr);
		 }
	 }

	 public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans)
	 {
		 return new Ganji(stepMeta, stepDataInterface, cnr, tr, trans);
	 }

	 public StepDataInterface getStepData()
	 {
		 return new GanjiData();
	 }

	 public void setVersionName(String versionName) {
		 version = versionName;
	 }

	 public String getVersionName() {
		 return version;
	 }

	 public void setRowLimit(long long1) {
		 rowLimit = long1;
	 }

	 public void setUrls(String[] items) {
		 urls = items;
	 }

	 public void setRequired(String[] items) {
		 required  =items;
	 }

	 public void allocate(int nrfiles, int nrkeys) {
		 urls = new String[nrfiles];
		 listPageCount = new String[nrfiles];
		 required = new String[nrfiles];
		 keyNames = new String[nrkeys];
		 fieldNames = new String[nrkeys];
	 }

	 public String[] getUrls() {
		 return urls;
	 }

	 public String[] getRequired() {
		 return required;
	 }

	 public long getRowLimit() {
		 return rowLimit;
	 }

	 public boolean isIncludeRowNumber() {
		 return includeRowNumber;
	 }

	 public void setIncludeRowNumber(boolean includeRowNumber) {
		 this.includeRowNumber = includeRowNumber;
	 }

	 public String getRowNumberField() {
		 return rowNumberField;
	 }

	 public void setRowNumberField(String rowNumberField) {
		 this.rowNumberField = rowNumberField;
	 }

	 //import!
	 public StepDialogInterface getDialog(Shell shell, StepMetaInterface info,
										  TransMeta transMeta, String name) {
		 return new GanjiDialog(shell, info, transMeta, name);
	 }

	 public String[] getListPageCount() {
		 return listPageCount;
	 }

	 public void setListPageCount(String[] listPageCount) {
		 this.listPageCount = listPageCount;
	 }

	 public String[] getKeyNames() {
		 return keyNames;
	 }

	 public void setKeyNames(String[] keyNames) {
		 this.keyNames = keyNames;
	 }

	 public String[] getFieldNames() {
		 return fieldNames;
	 }

	 public void setFieldNames(String[] fieldNames) {
		 this.fieldNames = fieldNames;
	 }
 }
