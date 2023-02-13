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

 package org.pentaho.di.trans.steps.crawler2020;

 import java.util.List;
 import java.util.Map;

 import org.pentaho.di.core.CheckResult;
 import org.pentaho.di.core.CheckResultInterface;
 import org.pentaho.di.core.Counter;
 import org.pentaho.di.core.database.DatabaseMeta;
 import org.pentaho.di.core.exception.KettleException;
 import org.pentaho.di.core.exception.KettleStepException;
 import org.pentaho.di.core.exception.KettleXMLException;
 import org.pentaho.di.core.row.RowMetaInterface;
 import org.pentaho.di.core.variables.VariableSpace;
 import org.pentaho.di.core.xml.XMLHandler;
 import org.pentaho.di.repository.Repository;
 import org.pentaho.di.trans.Trans;
 import org.pentaho.di.trans.TransMeta;
 import org.pentaho.di.trans.step.BaseStepMeta;
 import org.pentaho.di.trans.step.StepDataInterface;
 import org.pentaho.di.trans.step.StepInterface;
 import org.pentaho.di.trans.step.StepMeta;
 import org.pentaho.di.trans.step.StepMetaInterface;
 import org.pentaho.di.repository.ObjectId;
 import org.pentaho.di.i18n.BaseMessages;
 import org.pentaho.di.trans.steps.socketwriter.SocketWriter;
 import org.pentaho.di.trans.steps.socketwriter.SocketWriterData;
 import org.w3c.dom.Node;




 /**
  * Web site info extractor
  *
  * @author Jason
  * @since 10-Aug-2010
  */

 public class CrawlerInputMeta extends BaseStepMeta implements StepMetaInterface
 {
  private static Class<?> PKG = CrawlerInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$


  private String startPageURL;
  private String listPageURLPattern;
  private String contentPageURLPattern;
  private boolean compressed;

  public CrawlerInputMeta()
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

  public String getXML()
  {
   StringBuffer xml = new StringBuffer();

   xml.append("     "+XMLHandler.addTagValue("start_page_url", startPageURL));
   xml.append("     "+XMLHandler.addTagValue("content_page_url_pattern", contentPageURLPattern));
   xml.append("     "+XMLHandler.addTagValue("list_page_url_pattern", listPageURLPattern));
   xml.append("     "+XMLHandler.addTagValue("compressed", compressed));

   return xml.toString();
  }

  private void readData(Node stepnode)
  {
   contentPageURLPattern = XMLHandler.getTagValue(stepnode, "content_page_url_pattern");
   startPageURL = XMLHandler.getTagValue(stepnode, "start_page_url");
   listPageURLPattern = XMLHandler.getTagValue(stepnode, "list_page_url_pattern");
   compressed = "Y".equalsIgnoreCase( XMLHandler.getTagValue(stepnode, "compressed") );
  }

  public void setDefault()
  {
   startPageURL = "2000";
   listPageURLPattern = "5000";
   compressed = true;
  }

  public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException
  {
   contentPageURLPattern = rep.getStepAttributeString (id_step, "content_page_url_pattern");
   startPageURL = rep.getStepAttributeString (id_step, "start_page_url");
   listPageURLPattern = rep.getStepAttributeString (id_step, "list_page_url_pattern");
   compressed    = rep.getStepAttributeBoolean(id_step, "compressed");
  }

  public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException
  {
   rep.saveStepAttribute(id_transformation, id_step, "content_page_url_pattern", contentPageURLPattern);
   rep.saveStepAttribute(id_transformation, id_step, "start_page_url", startPageURL);
   rep.saveStepAttribute(id_transformation, id_step, "list_page_url_pattern", listPageURLPattern);
   rep.saveStepAttribute(id_transformation, id_step, "compressed", compressed);
  }

  public void getFields(RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) throws KettleStepException
  {
   // Default: nothing changes to rowMeta
  }

  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepinfo, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
  {
   CheckResult cr;
   if (prev==null || prev.size()==0)
   {
    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(PKG, "SocketWriterMeta.CheckResult.NotReceivingFields"), stepinfo); //$NON-NLS-1$
    remarks.add(cr);
   }
   else
   {
    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "SocketWriterMeta.CheckResult.StepRecevingData",prev.size()+""), stepinfo); //$NON-NLS-1$ //$NON-NLS-2$
    remarks.add(cr);
   }

   // See if we have input streams leading to this step!
   if (input.length>0)
   {
    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG, "SocketWriterMeta.CheckResult.StepRecevingData2"), stepinfo); //$NON-NLS-1$
    remarks.add(cr);
   }
   else
   {
    cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "SocketWriterMeta.CheckResult.NoInputReceivedFromOtherSteps"), stepinfo); //$NON-NLS-1$
    remarks.add(cr);
   }
  }

  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans)
  {
   return new SocketWriter(stepMeta, stepDataInterface, cnr, tr, trans);
  }

  public StepDataInterface getStepData()
  {
   return new SocketWriterData();
  }

  /**
   * @return the port
   */
  public String getContentPageURLPattern()
  {
   return contentPageURLPattern;
  }

  /**
   * @param contentPageURLPattern the port to set
   */
  public void setContentPageURLPattern(String contentPageURLPattern)
  {
   this.contentPageURLPattern = contentPageURLPattern;
  }

  public String getStartPageURL()
  {
   return startPageURL;
  }

  public void setStartPageURL(String startPageURL)
  {
   this.startPageURL = startPageURL;
  }

  public String getListPageURLPattern()
  {
   return listPageURLPattern;
  }

  public void setListPageURLPattern(String listPageURLPattern)
  {
   this.listPageURLPattern = listPageURLPattern;
  }

  /**
   * @return the compressed
   */
  public boolean isCompressed()
  {
   return compressed;
  }

  /**
   * @param compressed the compressed to set
   */
  public void setCompressed(boolean compressed)
  {
   this.compressed = compressed;
  }


 }