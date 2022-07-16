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

 import java.io.FileNotFoundException;
 import java.net.URL;
 import java.util.ArrayList;
 import java.util.Date;

 import org.pentaho.di.core.exception.KettleException;
 import org.pentaho.di.core.row.RowMeta;
 import org.pentaho.di.core.row.RowMetaInterface;
 import org.pentaho.di.core.row.ValueMeta;
 import org.pentaho.di.core.row.ValueMetaInterface;
 import org.pentaho.di.trans.Trans;
 import org.pentaho.di.trans.TransMeta;
 import org.pentaho.di.trans.step.BaseStep;
 import org.pentaho.di.trans.step.StepDataInterface;
 import org.pentaho.di.trans.step.StepInterface;
 import org.pentaho.di.trans.step.StepMeta;
 import org.pentaho.di.trans.step.StepMetaInterface;
// import org.pentaho.di.trans.steps.crawler2020.Messages;
 import org.pentaho.di.i18n.BaseMessages;

 import com.xgn.search.parser.ContentPage;
 import com.xgn.search.parser.ListPage;
 import com.xgn.search.parser.ganji.GanjiListPage;

 /**
  * web info extractor
  *
  * @author Jason
  * @since 10-Aug-2010
  */
 public class Crawler2020 extends BaseStep implements StepInterface
 {
	 private Crawler2020Meta meta;
	 private Crawler2020Data data;

	 public Crawler2020(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	 {
		 super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	 }

	 public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	 {
		 meta=(Crawler2020Meta)smi;
		 data=(Crawler2020Data)sdi;

		 ListPage lp = null;
		 try {
			 lp = new GanjiListPage();
			 //todo: put pattern in property file
			 //http://short.58.com/zd_p/8806c6bf-75a8-4748-a499-3317c92d6f04/?target=dc-bmocj-xgk_imob7_8508911411q-eyk&end=end
			 // href='http://short.58.com/zd_p/f967d196-bf8f-4475-bc64-e5218ea96042/?target=dc-bmocj-xgk_imob7_8186270663q-eyk&end=end'
			 //lp.setContentPagePattern("<[a|A][\\s]*[\\d|_|a-z|A-Z|-|\\|/|\\?|:|=]*short\\.58\\.com[\\d|_|a-z|A-Z|-|\\|/|\\?|:|=]*s\\s");
			 lp.setContentPagePattern("<[a|A]\\s[.]*short\\.58\\.com[.]*\\s");
		 }
		 catch (Exception ex) {
			 logError(BaseMessages.getString("Ganji.Log.ErrorOccurred") + ex.getMessage()); //$NON-NLS-1$
			 return false;
		 }

		 for (int i = 0; i<meta.getUrls().length;i++)
		 {
			 if(!meta.getRequired()[i].equals(BaseMessages.getString("System.Combo.Yes")))
				 continue;
			 for (int p=0;p<new Integer(meta.getListPageCount()[i]); p++)
			 {
				 try {
					 if(p==0)
						 lp.setUrl(new URL(meta.getUrls()[i]));
					 else
						 lp = lp.getNextListPage(lp.getUrl().toString());
					 lp.init();
					 lp.initContentPages();
				 }
				 catch (Exception ex) {
					 logError(BaseMessages.getString("Ganji.Log.ErrorOccurred") + ex.fillInStackTrace()); //$NON-NLS-1$
					 continue;
				 }

				 try {
					 ArrayList<ContentPage> contentPages= lp.getContentPages();
					 for(int j=0;j<contentPages.size();j++  )
					 {
						 Object[] row = getRowFromContent(contentPages.get(j));
						 putRow(data.ouputRowMeta, row);
						 data.rowCount++;
						 if(data.rowCount >= meta.getRowLimit())
						 {
							 setOutputDone();  // signal end to receiver(s)
							 return false;
						 }
					 }
				 }
				 catch (Exception ex) {
					 logError(BaseMessages.getString("Ganji.Log.ErrorOccurred") + ex.fillInStackTrace()); //$NON-NLS-1$

				 }

				 try {
					 Thread.sleep(100);
				 }
				 catch (InterruptedException ex) {
					 ex.printStackTrace();
				 }

			 }   // for list pages
		 }       //for urls

		 if (checkFeedback(getLinesRead()))
		 {
			 if(log.isBasic()) logBasic(BaseMessages.getString("Ganji.Log.LineNumber")+getLinesRead()); //$NON-NLS-1$
		 }
		 setOutputDone();  // signal end to receiver(s)
		 return false;
	 }

	 private Object[] getRowFromContent(ContentPage contentPage) {
		 Object[] result = new Object[meta.getFieldNames().length+4]; //three inner field!
		 for(int i=0;i<meta.getKeyNames().length;i++)
		 {
			 String value = contentPage.getProperty(meta.getKeyNames()[i]);
			 result[i]=value;
		 }
		 result[meta.getFieldNames().length]=contentPage.getTitle();
		 result[meta.getFieldNames().length+1]=contentPage.guessTime();
		 String phone = contentPage.guessPhone();
		 if(phone!=null && phone.length()>2 && meta.getVersionName().equals(meta.VERSION_FREE))//mask phone if it 's a free version
			 phone = phone.substring(0,phone.length()-2)+"XX";
		 result[meta.getFieldNames().length+2]=phone;
		 result[meta.getFieldNames().length+3]=contentPage.getUrl().toString();
		 return result;
	 }

	 public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	 {
		 meta=(Crawler2020Meta)smi;
		 data=(Crawler2020Data)sdi;

		 if (super.init(smi, sdi))
		 {

			 if(new Date().getYear()>2011)
			 {
				 //log.logBasic("??????????????????","???????????????????????????????? "+GanjiMeta.RESTRICT_MAX_LINE+" ??????????????????????}???q???????????????????? support@pentahochina.com ???????????" );
				 meta.setVersionName(Crawler2020Meta.VERSION_FREE);
				 meta.setRowLimit(meta.RESTRICT_MAX_LINE);
			 }

			 RowMetaInterface outputRowMeta = new RowMeta();
			 int stringType = ValueMeta.getType("String");

			 for(int i=0;i<meta.getFieldNames().length;i++)
			 {
				 ValueMetaInterface fieldMeta = new ValueMeta( meta.getFieldNames()[i], stringType);
				 fieldMeta.setLength(-1);
				 outputRowMeta.addValueMeta(fieldMeta);
			 }

			 ValueMetaInterface titleMeta = new ValueMeta( "title_inner", stringType);
			 titleMeta.setLength(-1);
			 ValueMetaInterface timeMeta = new ValueMeta( "time_inner", stringType);
			 timeMeta.setLength(-1);
			 ValueMetaInterface phoneMeta = new ValueMeta( "phone_inner", stringType);
			 phoneMeta.setLength(-1);
			 ValueMetaInterface urlMeta = new ValueMeta( "url_inner", stringType);
			 urlMeta.setLength(-1);

			 outputRowMeta.addValueMeta(titleMeta);
			 outputRowMeta.addValueMeta(timeMeta);
			 outputRowMeta.addValueMeta(phoneMeta);
			 outputRowMeta.addValueMeta(urlMeta);
			 data.ouputRowMeta =outputRowMeta;
			 return true;
		 }
		 return false;
	 }

	 public void dispose(StepMetaInterface smi, StepDataInterface sdi)
	 {
		 super.dispose(smi, sdi);
	 }

//	public void run()
//	{
	 //   	BaseStep.runStepThread(this, meta, data);
//	}
 }
