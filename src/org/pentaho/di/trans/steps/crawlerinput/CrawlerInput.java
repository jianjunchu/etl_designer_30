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

 package org.pentaho.di.trans.steps.crawlerinput;

 import java.util.ArrayList;
 import java.util.HashSet;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;

 import com.xgn.search.parser.AbstractListPage;
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
 import org.pentaho.di.i18n.BaseMessages;

 /**
  * web info extractor
  *
  * @author Jason
  * @since 10-Aug-2010
  */
 public class CrawlerInput extends BaseStep implements StepInterface
 {
	 private static Class<?> PKG = CrawlerInputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	 private CrawlerInputMeta meta;
	 private CrawlerInputData data;


	 public CrawlerInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	 {
		 super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	 }

	 public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	 {
		 meta=(CrawlerInputMeta)smi;
		 data=(CrawlerInputData)sdi;
		 boolean retval=true;
		 if(first)
		 {
			 first = false;
			 String startURL = environmentSubstitute(meta.getStartPageURL());
			 try {
//				 HTMLTractor extractor = new HTMLTractor(startURL);
//				 String startPageContent = extractor.getSource("utf-8");
//				 extractListPage(startPageContent);
				 ArrayList<Object[]> RowsInStartPage = extractContentPages(startURL);
				 for (int i=0 ; i<RowsInStartPage.size();i++)
				 {
					 Object[] r = RowsInStartPage.get(i);
					 data.contentRowQueue.add(r);
				 }
				 if(data.listPageQueue.size()>0)
				 {
					 String listPageURL =data.listPageQueue.poll();
					 ArrayList<Object[]> rows = extractContentPages(listPageURL);
					 for (int i=0 ; i<rows.size();i++)
					 {
						 Object[] r = rows.get(i);
						 data.contentRowQueue.add(r);
					 }
				 }
				 //process(data.listPageQueue);
			 }catch (Exception e)
			 {
				 e.printStackTrace();
			 }
			 if (checkFeedback(getLinesRead()))
			 {
				 if(log.isBasic()) logBasic(BaseMessages.getString("CrawlerInput.Log.LineNumber")+getLinesRead()); //$NON-NLS-1$
			 }
		 }
		 else if (data.listPageQueue.size()>0 && data.contentRowQueue.size()<meta.getContentQueueBufferSize()) //continue to extract
		 {
			 try {
				 String listPageURL =data.listPageQueue.poll();
				 ArrayList<Object[]> rows = extractContentPages(listPageURL);//extract content pages in the list page and extract other list pages
				 for (int i=0 ; i<rows.size();i++)
				 {
					 Object[] r = rows.get(i);
					 data.contentRowQueue.add(r);
				 }
			 }catch (Exception e)
			 {
				 e.printStackTrace();
			 }

			 if (checkFeedback(getLinesRead()))
			 {
				 if(log.isBasic()) logBasic(BaseMessages.getString("CrawlerInput.Log.LineNumber")+getLinesRead()); //$NON-NLS-1$
			 }
		 }

		 if(data.rowLimit<0 || data.rowsWritten<data.rowLimit) {
			 if(data.contentRowQueue.size()>0) {
				 Object[] r = data.contentRowQueue.poll();
				 putRow(data.outputRowMeta, r);
				 data.rowsWritten++;
				 if (log.isRowLevel()) {
					 logRowlevel(BaseMessages.getString(PKG, "CrawlerInput.Log.LineNumber", Long.toString(data.rowsWritten), data.outputRowMeta.getString(r)));
				 }else if(log.isBasic())
				 {
					 logBasic(r[1].toString());
				 }
				 if (checkFeedback(data.rowsWritten)) {
					 if (log.isBasic())
						 logBasic(BaseMessages.getString(PKG, "CrawlerInput.Log.LineNumber", Long.toString(data.rowsWritten)));
				 }
			 }else {
				 setOutputDone();  // signal end to receiver(s)
				 return false;
			 }
		 }else{
			 setOutputDone();  // signal end to receiver(s)
			 return false;
		 }

		 return retval;
	 }

	 /**
	  * extract Content Pages to results and extract list Page urls to queue
	  * @param   fromPageUrl
	  * @return
	  */
	 private ArrayList<Object[]> extractContentPages(String fromPageUrl) {
		 ArrayList list = new ArrayList();
		 try {
			 HTMLTractor listPageTractor = new HTMLTractor(fromPageUrl);
			 String pageContent = listPageTractor.getSource("utf-8");
			 extractListPage(pageContent,fromPageUrl);//extract list page from html
			 String contentPageUrl =environmentSubstitute(meta.getContentPageURLPattern());
			 list = extractContentPageImpl(fromPageUrl,contentPageUrl,pageContent);
			 if(list.size()==0&&contentPageUrl.indexOf("http")>-1)
			 {
				 contentPageUrl = contentPageUrl.substring(contentPageUrl.indexOf("/",contentPageUrl.indexOf("//")+2));
				 list = extractContentPageImpl(fromPageUrl,contentPageUrl,pageContent);
			 }

		 } catch (Exception e) {
			 throw new RuntimeException(e);
		 }
		 return list;
	 }

	 private ArrayList extractContentPageImpl(String fromPageUrl, String contentPageUrl,String pageContent) {
		 ArrayList list = new ArrayList();
		 HashSet extractedUrl = new HashSet();
		 Pattern contentPagePattern = Pattern.compile(contentPageUrl);
		 Matcher contentPageMatcher = contentPagePattern.matcher(pageContent);
		 int count=0;
		 try {
			 while (contentPageMatcher.find()) {
				 count++;
				 String contentURL = contentPageMatcher.group();
				 String url;
				 if (contentURL.startsWith("http")) {
					 url = contentURL;
				 } else if (contentURL.startsWith("/")) //matched absolut path
				 {
					 url = getDomainName(fromPageUrl) + contentURL; //fromPageUrl should start with http:// or https://
				 } else {
					 url = fromPageUrl.substring(0, fromPageUrl.lastIndexOf("/") + 1) + contentURL;
				 }
				 if(!extractedUrl.contains(url)) {
					 HTMLTractor tractor = new HTMLTractor(url);
					 String content = tractor.getSource("utf-8");//extract content page content
					 Object[] row = new Object[4];
					 row[0] = url;//url
					 row[1] = contentURL.substring(contentURL.lastIndexOf("/") + 1, contentURL.length());//file name
					 row[2] = content;//content
					 list.add(row);
					 extractedUrl.add(url);//url saved to already extracted set
				 }
			 }
		 }catch(Exception e)
		 {
			 e.printStackTrace();
		 }
		 return list;
	 }

	 /**
	  *
	  * @param pageContent
	  * @param fromPageUrl
	  */
	 private void extractListPage(String pageContent,String fromPageUrl) {
		 String listPageURLPattern = environmentSubstitute(meta.getListPageURLPattern());
		 int count = extractListPageImpl( pageContent, fromPageUrl, listPageURLPattern);
		 if(count==0 && listPageURLPattern.startsWith("http"))//if not found, remove http in list page pattern, and try again
		 {
			 listPageURLPattern = listPageURLPattern.substring(listPageURLPattern.indexOf("/",listPageURLPattern.indexOf("//")+2));
			 extractListPageImpl( pageContent, fromPageUrl, listPageURLPattern);
		 }
	 }

	 private int extractListPageImpl(String pageContent, String fromPageUrl, String listPageURLPattern) {
		 Pattern pattern = Pattern.compile(listPageURLPattern);
		 Matcher matcher = pattern.matcher(pageContent);
		 int count=0;
		 while (matcher.find()) {
			 count++;
			 String matched = matcher.group();
			 String url = null;
			 if(matched.startsWith("http") )
			 {
				 url= matched;
			 }
			 else if(matched.startsWith("/") ) //matched absolut path
			 {
				 url=  getDomainName(fromPageUrl)+matched; //fromPageUrl should start with http:// or https://
			 }else
			 {
				 url= fromPageUrl.substring(0,fromPageUrl.lastIndexOf("/")+1)+matched;
			 }
			 if(!data.allListPage.contains(url)) {
				 data.allListPage.add(url);
				 data.listPageQueue.add(url);
			 }
		 }
		 return count;
	 }

	 public String getDomainName(String url)
	 {
		 int index = url.indexOf("//");
		 if(index ==-1)
			return null;
		 return url.substring(0,url.indexOf("/",index+3));
	 }

//	 public AbstractListPage getNextListPage(String listPageURLPatern,String content) throws Exception {
//		 if (!listPageURLPatern.endsWith("/")) {
//			 listPageURLPatern = listPageURLPatern + "/";
//		 }
//
//		 Pattern pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+f[0-9]{1,2}/");
//		 Matcher matcher = pattern.matcher(listPageURLPatern);
//		 if (matcher.find()) {
//			 //return this.getNextListPageByCanonicalURL(listPageURLPatern);
//		 } else {
////			 pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+[a-z|0-9]+f[0-9]{1,2}/");
////			 matcher = pattern.matcher(listPageURLPatern);
////			 if (matcher.find()) {
////				 return this.getNextListPageByCanonicalURL(listPageURLPatern);
////			 } else {
////				 pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+f[0-9]{1,2}/");
////				 matcher = pattern.matcher(content);
////				 if (matcher.find()) {
////					 return this.getNextListPageByCanonicalURL(listPageURLPatern.toString() + "f0/");
////				 } else {
////					 pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+[a-z|0-9]+f[0-9]{1,2}/");
////					 matcher = pattern.matcher(content);
////					 return matcher.find() ? this.getNextListPageByCanonicalURL(listPageURLPatern.substring(0, listPageURLPatern.length() - 1) + "f0" + "/") : null;
////				 }
////			 }
//		 }
//		 return null;
//	 }

	 public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	 {
		 meta=(CrawlerInputMeta)smi;
		 data=(CrawlerInputData)sdi;

		 if (super.init(smi, sdi))
		 {
			 data.rowLimit = meta.getRowLimit();
			 data.rowsWritten = 0;

			 RowMetaInterface outputRowMeta = new RowMeta();
			 int stringType = ValueMeta.getType("String");
			 int intType = ValueMeta.getType("Integer");
			 ValueMetaInterface urlMeta = new ValueMeta( "url", stringType);
			 urlMeta.setLength(-1);
			 ValueMetaInterface fileNameMeta = new ValueMeta( "file_name", stringType);
			 fileNameMeta.setLength(-1);
			 ValueMetaInterface contentMeta = new ValueMeta( "content", stringType);
			 contentMeta.setLength(-1);
			 outputRowMeta.addValueMeta(urlMeta);
			 outputRowMeta.addValueMeta(fileNameMeta);
			 outputRowMeta.addValueMeta(contentMeta);
			 data.outputRowMeta =outputRowMeta;
			 return true;
		 }
		 return false;
	 }

	 public void dispose(StepMetaInterface smi, StepDataInterface sdi)
	 {
		 super.dispose(smi, sdi);
	 }




 }
=======
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

 import java.util.ArrayList;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;

 import com.xgn.search.html.HTMLTractor;
 import com.xgn.search.parser.AbstractListPage;
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

 /**
  * web info extractor
  *
  * @author Jason
  * @since 10-Aug-2010
  */
 public class CrawlerInput extends BaseStep implements StepInterface
 {
	 private CrawlerInputMeta meta;
	 private CrawlerInputData data;
	 private int id=0;

	 public CrawlerInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	 {
		 super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	 }

	 public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	 {
		 meta=(CrawlerInputMeta)smi;
		 data=(CrawlerInputData)sdi;
		 String startURL = meta.getStartPageURL();
		 try {
			 HTMLTractor extractor = new HTMLTractor(startURL);
			 String startContent = extractor.getSource("utf-8");
			 Pattern pattern = Pattern.compile(meta.getListPageURLPattern());
			 Matcher matcher = pattern.matcher(startContent);
			 while (matcher.find()) {
				 String url = matcher.group();
				 System.out.println(matcher.group());
				 if(!data.allListPage.contains(url)) {
					 data.allListPage.add(url);
					 data.listPageQueue.add(url);
				 }
				 //return this.getNextListPageByCanonicalURL(listPageURLPatern);
			 }
			 while(data.listPageQueue.size()>0)
			 {
				 String listPageURL =data.listPageQueue.poll();
				 ArrayList<Object[]> rows = processListPage(listPageURL);
				 for (int i=0 ; i<rows.size();i++)
				 {
					 this.putRow(data.ouputRowMeta,rows.get(i));
				 }
			 }
			 //process(data.listPageQueue);
		 }catch (Exception e)
		 {
			 e.printStackTrace();
		 }
//		 ListPage lp = null;
//		 try {
//			 lp = new GanjiListPage();
//			 //todo: put pattern in property file
//			 //http://short.58.com/zd_p/8806c6bf-75a8-4748-a499-3317c92d6f04/?target=dc-bmocj-xgk_imob7_8508911411q-eyk&end=end
//			 // href='http://short.58.com/zd_p/f967d196-bf8f-4475-bc64-e5218ea96042/?target=dc-bmocj-xgk_imob7_8186270663q-eyk&end=end'
//			 //lp.setContentPagePattern("<[a|A][\\s]*[\\d|_|a-z|A-Z|-|\\|/|\\?|:|=]*short\\.58\\.com[\\d|_|a-z|A-Z|-|\\|/|\\?|:|=]*s\\s");
//			 lp.setContentPagePattern("<[a|A]\\s[.]*short\\.58\\.com[.]*\\s");
//		 }
//		 catch (Exception ex) {
//			 logError(BaseMessages.getString("Ganji.Log.ErrorOccurred") + ex.getMessage()); //$NON-NLS-1$
//			 return false;
//		 }
//
//		 for (int i = 0; i<meta.getUrls().length;i++)
//		 {
//			 if(!meta.getRequired()[i].equals(BaseMessages.getString("System.Combo.Yes")))
//				 continue;
//			 for (int p=0;p<new Integer(meta.getListPageCount()[i]); p++)
//			 {
//				 try {
//					 if(p==0)
//						 lp.setUrl(new URL(meta.getUrls()[i]));
//					 else
//						 lp = lp.getNextListPage(lp.getUrl().toString());
//					 lp.init();
//					 lp.initContentPages();
//				 }
//				 catch (Exception ex) {
//					 logError(BaseMessages.getString("Ganji.Log.ErrorOccurred") + ex.fillInStackTrace()); //$NON-NLS-1$
//					 continue;
//				 }
//
//				 try {
//					 ArrayList<ContentPage> contentPages= lp.getContentPages();
//					 for(int j=0;j<contentPages.size();j++  )
//					 {
//						 Object[] row = getRowFromContent(contentPages.get(j));
//						 putRow(data.ouputRowMeta, row);
//						 data.rowCount++;
//						 if(data.rowCount >= meta.getRowLimit())
//						 {
//							 setOutputDone();  // signal end to receiver(s)
//							 return false;
//						 }
//					 }
//				 }
//				 catch (Exception ex) {
//					 logError(BaseMessages.getString("Ganji.Log.ErrorOccurred") + ex.fillInStackTrace()); //$NON-NLS-1$
//
//				 }
//				 try {
//					 Thread.sleep(100);
//				 }
//				 catch (InterruptedException ex) {
//					 ex.printStackTrace();
//				 }
//			 }   // for list pages
//		 }       //for urls

		 if (checkFeedback(getLinesRead()))
		 {
			 if(log.isBasic()) logBasic(BaseMessages.getString("Ganji.Log.LineNumber")+getLinesRead()); //$NON-NLS-1$
		 }
		 setOutputDone();  // signal end to receiver(s)
		 return false;
	 }

	 private ArrayList<Object[]> processListPage(String listPageURL) {
		 ArrayList list = new ArrayList();
		 try {
			 HTMLTractor listPageTractor = new HTMLTractor(listPageURL);
			 String pageContent = listPageTractor.getSource("utf-8");
			 Pattern contentPagePattern = Pattern.compile(meta.getContentPageURLPattern());
			 Matcher contentPageMatcher = contentPagePattern.matcher(pageContent);
			 while (contentPageMatcher.find()) {
				 String contentURL = contentPageMatcher.group();
				 System.out.println(contentURL);
				 HTMLTractor tractor = new HTMLTractor(contentURL);
				 String content = tractor.getSource("utf-8");
				 //return this.getNextListPageByCanonicalURL(listPageURLPatern);
				 Object[] row = new Object[5];
				 row[0] = id+1; //id
				 row[1] = contentURL;//url
				 row[2] = "";//title
				 row[3] = content;//content
				 row[4] = System.currentTimeMillis();//time
				 list.add(row);
			 }
		 } catch (Exception e) {
			 throw new RuntimeException(e);
		 }
		 return list;
	 }


	 public AbstractListPage getNextListPage(String listPageURLPatern,String content) throws Exception {
		 if (!listPageURLPatern.endsWith("/")) {
			 listPageURLPatern = listPageURLPatern + "/";
		 }

		 Pattern pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+f[0-9]{1,2}/");
		 Matcher matcher = pattern.matcher(listPageURLPatern);
		 if (matcher.find()) {
			 //return this.getNextListPageByCanonicalURL(listPageURLPatern);
		 } else {
//			 pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+[a-z|0-9]+f[0-9]{1,2}/");
//			 matcher = pattern.matcher(listPageURLPatern);
//			 if (matcher.find()) {
//				 return this.getNextListPageByCanonicalURL(listPageURLPatern);
//			 } else {
//				 pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+f[0-9]{1,2}/");
//				 matcher = pattern.matcher(content);
//				 if (matcher.find()) {
//					 return this.getNextListPageByCanonicalURL(listPageURLPatern.toString() + "f0/");
//				 } else {
//					 pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+[a-z|0-9]+f[0-9]{1,2}/");
//					 matcher = pattern.matcher(content);
//					 return matcher.find() ? this.getNextListPageByCanonicalURL(listPageURLPatern.substring(0, listPageURLPatern.length() - 1) + "f0" + "/") : null;
//				 }
//			 }
		 }
		 return null;
	 }
//
//	 public AbstractListPage getNextListPageByCanonicalURL(String url) throws Exception {
//		 if (!url.endsWith("/")) {
//			 url = url + "/";
//		 }
//
//		 Pattern pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+f\\d+/");
//		 Matcher matcher = pattern.matcher(url);
//		 int d;
//		 if (matcher.find()) {
//			 d = new Integer(url.substring(url.lastIndexOf("f") + 1, url.length() - 1));
//			 return this.getListPage(url.substring(0, url.lastIndexOf("f") + 1) + (new Integer(d + 32)).toString() + "/");
//		 } else {
//			 pattern = Pattern.compile("http://[a-z|0-9]{1,10}\\.ganji\\.com/(([a-z|0-9])+/)+f\\d+p[0-9]{1,2}/");
//			 matcher = pattern.matcher(url);
//			 if (matcher.find()) {
//				 d = new Integer(url.substring(url.lastIndexOf("f") + 1, url.lastIndexOf("p")));
//				 return this.getListPage(url.substring(0, url.lastIndexOf("f") + 1) + (new Integer(d + 32)).toString() + url.substring(url.lastIndexOf("p")));
//			 } else {
//				 return null;
//			 }
//		 }
//	 }

//	 private Object[] getRowFromContent(ContentPage contentPage) {
//		 Object[] result = new Object[meta.getFieldNames().length+4]; //three inner field!
//		 for(int i=0;i<meta.getKeyNames().length;i++)
//		 {
//			 String value = contentPage.getProperty(meta.getKeyNames()[i]);
//			 result[i]=value;
//		 }
//		 result[meta.getFieldNames().length]=contentPage.getTitle();
//		 result[meta.getFieldNames().length+1]=contentPage.guessTime();
//		 String phone = contentPage.guessPhone();
//		 if(phone!=null && phone.length()>2 )//mask phone if it 's a free version
//			 phone = phone.substring(0,phone.length()-2)+"XX";
//		 result[meta.getFieldNames().length+2]=phone;
//		 result[meta.getFieldNames().length+3]=contentPage.getUrl().toString();
//		 return result;
//	 }

	 public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	 {
		 meta=(CrawlerInputMeta)smi;
		 data=(CrawlerInputData)sdi;

		 if (super.init(smi, sdi))
		 {

			 RowMetaInterface outputRowMeta = new RowMeta();
			 int stringType = ValueMeta.getType("String");
			 int intType = ValueMeta.getType("Integer");

//			 for(int i=0;i<meta.getFieldNames().length;i++)
//			 {
//				 ValueMetaInterface fieldMeta = new ValueMeta( meta.getFieldNames()[i], stringType);
//				 fieldMeta.setLength(-1);
//				 outputRowMeta.addValueMeta(fieldMeta);
//			 }

//			 ValueMetaInterface titleMeta = new ValueMeta( "title_inner", stringType);
//			 titleMeta.setLength(-1);
//			 ValueMetaInterface timeMeta = new ValueMeta( "time_inner", stringType);
//			 timeMeta.setLength(-1);
//			 ValueMetaInterface phoneMeta = new ValueMeta( "phone_inner", stringType);
//			 phoneMeta.setLength(-1);
//			 ValueMetaInterface urlMeta = new ValueMeta( "url_inner", stringType);
//			 urlMeta.setLength(-1);

			 ValueMetaInterface idMeta = new ValueMeta( "id", intType);
			 idMeta.setLength(-1);
			 ValueMetaInterface urlMeta = new ValueMeta( "url_inner", stringType);
			 urlMeta.setLength(-1);
			 ValueMetaInterface titleMeta = new ValueMeta( "title_inner", stringType);
			 titleMeta.setLength(-1);
			 ValueMetaInterface contentMeta = new ValueMeta( "content", stringType);
			 contentMeta.setLength(-1);
			 ValueMetaInterface timeMeta = new ValueMeta( "time_inner", stringType);
			 timeMeta.setLength(-1);

			 outputRowMeta.addValueMeta(idMeta);
			 outputRowMeta.addValueMeta(urlMeta);
			 outputRowMeta.addValueMeta(titleMeta);
			 outputRowMeta.addValueMeta(contentMeta);
			 outputRowMeta.addValueMeta(timeMeta);
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
>>>>>>> 061dd4d1380c5d44bf317a2ed89d12c587e8d64c:src/org/pentaho/di/trans/steps/crawler2020/CrawlerInput.java
