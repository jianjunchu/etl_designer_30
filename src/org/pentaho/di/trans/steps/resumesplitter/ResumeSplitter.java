/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.resumesplitter;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.resumesplitter.ResumeSplitterData;
import org.pentaho.di.trans.steps.resumesplitter.ResumeSplitterMeta;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;


/**
 * Split a single String fields into multiple parts based on certain conditions.
 * 
 * @author Matt
 * @since 31-Okt-2003
 * @author Daniel Einspanjer
 * @since 15-01-2008
 */
public class ResumeSplitter extends BaseStep implements StepInterface
{
	private static Class<?> PKG = ResumeSplitterMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private ResumeSplitterMeta meta;
	private ResumeSplitterData data;
	
	public ResumeSplitter(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	{
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}
	
	private ArrayList<Object[]> splitField(Object[] r) throws KettleValueException
	{
		if (first)
		{
			first=false;
			// get the RowMeta
			data.previousMeta = getInputRowMeta().clone();
			
			// search field
			data.fieldnr=data.previousMeta.indexOfValue(meta.getSplitField());
			if (data.fieldnr<0)
			{
				throw new KettleValueException(BaseMessages.getString(PKG, "ResumeSplitter.Log.CouldNotFindFieldToSplit",meta.getSplitField())); //$NON-NLS-1$ //$NON-NLS-2$
			}

			// only String type allowed
			if (!data.previousMeta.getValueMeta(data.fieldnr).isString())
			{
				throw new KettleValueException((BaseMessages.getString(PKG, "ResumeSplitter.Log.SplitFieldNotValid",meta.getSplitField()))); //$NON-NLS-1$ //$NON-NLS-2$
			}

			// prepare the outputMeta
			//
			data.outputMeta= getInputRowMeta().clone();
			meta.getFields(data.outputMeta, getStepname(), null, null, this);
			
			// Now create objects to do string to data type conversion...
			//
			data.conversionMeta = data.outputMeta.clone();
			for (ValueMetaInterface valueMeta : data.conversionMeta.getValueMetaList()) {
				valueMeta.setType(ValueMetaInterface.TYPE_STRING);
			}

		}
		
		String v=data.previousMeta.getString(r, data.fieldnr);
		ArrayList<ResumeSegment> list = splitResume(v);

//		for(ResumeSegment s : list)
//		{
//			System.out.println("Start Date："+s.getStartDate());
//			System.out.println("End Date："+s.getEndDate());
//			System.out.println("类型："+s.getResumeTypeDesc());
//			System.out.println("组织："+s.getOrgnization());
//			System.out.println("学历："+s.getStudyPosition());
//			System.out.println("职务："+s.getWorkPosition());
//			System.out.println();
//		}
//		System.out.println();
//		System.out.println();
		ArrayList outputRows = new ArrayList();
		// reserve room
		if(list==null || list.size()==0)//no result found, return original record
		{
			Object[] outputRow = RowDataUtil.allocateRowData(data.outputMeta.size());
			for (int i=0;i<getInputRowMeta().size();i++) outputRow[i] = r[i];
			outputRows.add(outputRow);
		}else
		for(ResumeSegment s : list)
		{
			Object[] outputRow = RowDataUtil.allocateRowData(data.outputMeta.size());
			for (int i=0;i<getInputRowMeta().size();i++) outputRow[i] = r[i];
			outputRow[getInputRowMeta().size()]=s.getStartDate();
			outputRow[getInputRowMeta().size()+1]=s.getEndDate();
			outputRow[getInputRowMeta().size()+2]=s.getResumeTypeDesc();
			outputRow[getInputRowMeta().size()+3]=s.getOrgnization();
			outputRow[getInputRowMeta().size()+4]=s.getStudyPosition();
			outputRow[getInputRowMeta().size()+5]=s.getWorkPosition();
			outputRows.add(outputRow);
		}

//		int nrExtraFields = meta.getFieldID().length - 1;
//
//		for (int i=0;i<data.fieldnr;i++) outputRow[i] = r[i];
//		for (int i=data.fieldnr+1;i<data.previousMeta.size();i++) outputRow[i+nrExtraFields] = r[i];
//
//		// OK, now we have room in the middle to place the fields...
//		//
//
//		// Named values info.id[0] not filled in!
//		boolean use_ids = meta.getFieldID().length>0 && meta.getFieldID()[0]!=null && meta.getFieldID()[0].length()>0;
//
//		Object value=null;
//		if (use_ids)
//		{
//			if (log.isDebug()) logDebug(BaseMessages.getString(PKG, "ResumeSplitter.Log.UsingIds")); //$NON-NLS-1$
//
//			// pol all split fields
//			// Loop over the specified field list
//			// If we spot the corresponding id[] entry in pol, add the value
//			//
//            int polSize = 0;
//            if (v != null)
//            {
//                polSize++;
//                for (int i = 0; i < v.length(); i++)
//                {
//                    i = v.indexOf(data.delimiter, i);
//                    if (i == -1) break;
//                    else polSize++;
//                }
//            }
//            final String pol[] = new String[polSize];
//			int prev=0;
//			int i=0;
//			while(v!=null && prev<v.length() && i<pol.length)
//			{
//                pol[i] = polNext(v, data.delimiter, prev);
//                if (log.isDebug())
//                    logDebug(BaseMessages.getString(PKG, "ResumeSplitter.Log.SplitFieldsInfo", pol[i], String.valueOf(prev))); //$NON-NLS-1$ //$NON-NLS-2$
//                prev += pol[i].length() + data.delimiter.length();
//				i++;
//			}
//
//			// We have to add info.field.length variables!
//            for (i = 0; i < meta.getFieldName().length; i++)
//			{
//				// We have a field, search the corresponding pol[] entry.
//				String split=null;
//
//				for (int p=0; p<pol.length && split==null; p++)
//				{
//					// With which line does pol[p] correspond?
//                    if (pol[p] != null)
//                    {
//                        if (Const.trimToType(pol[p], meta.getFieldTrimType()[i]).indexOf(meta.getFieldID()[i]) == 0)
//                            split = pol[p];
//                    }
//                }
//
//				// Optionally remove the indicator
//                if (split != null && meta.getFieldRemoveID()[i])
//				{
//                    final StringBuilder sb = new StringBuilder(split);
//                    final int idx = sb.indexOf(meta.getFieldID()[i]);
//					sb.delete(idx, idx+meta.getFieldID()[i].length());
//					split=sb.toString();
//				}
//
//				if (split==null) split=""; //$NON-NLS-1$
//				if (log.isDebug()) logDebug(BaseMessages.getString(PKG, "ResumeSplitter.Log.SplitInfo")+split); //$NON-NLS-1$
//
//				try
//				{
//					ValueMetaInterface valueMeta = data.outputMeta.getValueMeta(data.fieldnr+i);
//					ValueMetaInterface conversionValueMeta = data.conversionMeta.getValueMeta(data.fieldnr+i);
//					value = valueMeta.convertDataFromString
//					(
//						split,
//						conversionValueMeta,
//						meta.getFieldNullIf()[i],
//						meta.getFieldIfNull()[i],
//						meta.getFieldTrimType()[i]
//					);
//				}
//				catch(Exception e)
//				{
//					throw new KettleValueException(BaseMessages.getString(PKG, "ResumeSplitter.Log.ErrorConvertingSplitValue",split,meta.getSplitField()+"]!"), e); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
//				}
//				outputRow[data.fieldnr+i]=value;
//			}
//		}
//		else
//		{
//			if (log.isDebug()) logDebug(BaseMessages.getString(PKG, "ResumeSplitter.Log.UsingPositionOfValue")); //$NON-NLS-1$
//			int prev=0;
//			for (int i=0;i<meta.getFieldName().length;i++)
//			{
//				String pol = polNext(v, data.delimiter, prev);
//				if (log.isDebug()) logDebug(BaseMessages.getString(PKG, "ResumeSplitter.Log.SplitFieldsInfo",pol,String.valueOf(prev))); //$NON-NLS-1$ //$NON-NLS-2$
//				prev+=(pol==null?0:pol.length()) + data.delimiter.length();
//
//				try
//				{
//					ValueMetaInterface valueMeta = data.outputMeta.getValueMeta(data.fieldnr+i);
//					ValueMetaInterface conversionValueMeta = data.conversionMeta.getValueMeta(data.fieldnr+i);
//					value = valueMeta.convertDataFromString
//					(
//						pol,
//						conversionValueMeta,
//						meta.getFieldNullIf()[i],
//						meta.getFieldIfNull()[i],
//						meta.getFieldTrimType()[i]
//					);
//				}
//				catch(Exception e)
//				{
//					throw new KettleValueException(BaseMessages.getString(PKG, "ResumeSplitter.Log.ErrorConvertingSplitValue",pol,meta.getSplitField()+"]!"), e); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
//				}
//				outputRow[data.fieldnr+i]=value;
//			}
//		}
		
		return outputRows;
	}

	private ArrayList splitResume(String v)
	{
		if(v==null)
			return null;
		if(v.indexOf("<p>")>-1) //if found tag ,then trim
			v=trimTags(v);
		if(v.length()<2)
			return null;
		ArrayList list = new ArrayList();
		int offset =0;
		char currentChar;
		int state=0;//0:resume field,  1:start year field  2:start month field  3:end year field,  4:end month field
		String tempStartYear="";
		String tempStartMonth="";
		String tempEndYear="";
		String tempEndMonth="";
		String content="";
		while (offset<v.length())
		{
			currentChar = v.charAt(offset);
			if(state==0) //0:resume field
			{
				if (
						(currentChar == '1' && this.getNoneSpaceChar(v,offset,1) == '9' && isDigital(getNoneSpaceChar(v,offset,2)))
						|| (currentChar == '2' && this.getNoneSpaceChar(v,offset,1) == '0' && isDigital(getNoneSpaceChar(v,offset,2)))
						)
				{
					if(tempStartYear!=null && tempStartYear.length()>0)
					{
						ResumeSegment segment = new ResumeSegment();
						segment.setStartDate(createDate(tempStartYear,tempStartMonth));
						segment.setEndDate(createDate(tempEndYear,tempEndMonth));
						String workposition =  getWorkPosition(content);
						String studyposition =  getStudyPosition(content);
						segment.setWorkPosition(workposition);
						segment.setStudyPosition(studyposition);
						segment.setOrgnization(trimSymbols(content));
						segment.setResumeType(getResumeType(content,workposition,studyposition));
						//segment.setContent(trimSymbols(content));

						list.add(segment);

						tempStartYear="";
						tempStartMonth="";
						tempEndYear="";
						tempEndMonth="";
						content="";
					}
					tempStartYear+=currentChar;
					state = 1;
				}
				else if (currentChar=='《' ||(currentChar=='<' && this.getNoneSpaceChar(v,offset,1) == '<'))
				{
					content+=currentChar;
					state = 5;// in qutation
				}
				else
					content+=currentChar;
			}else if(state==1)//1:start year field
			{
				if(isDigital(currentChar))
				{
					tempStartYear+= currentChar;
				}else if(currentChar=='年' && this.getNoneSpaceChar(v,offset,1) =='第')
				{
					content+=tempStartYear;
					tempStartYear="";
					state=0;
				}
				else if(currentChar=='年'||currentChar=='-'||currentChar=='－'||currentChar=='至'||currentChar=='-')
				{
					 if(startWithMonth(v,offset+1))
					{
						offset = skipToFirstDigital(v,offset)-1;
						state=2;
					}
					else if(startWithYear(v,offset+1)) {
						offset = skipToFirstDigital(v,offset)-1;
						state = 3;
					}
					else
						state=0;

					if(currentChar=='年')
					{
						if(getFirstChar(v,offset+1)=='初'||getFirstChar(v,offset+1)=='中'||getFirstChar(v,offset+1)=='末')
						offset ++; //skip 初中未 char
					}
				}else if(currentChar=='.'||currentChar=='.')
				{
					offset = skipToFirstDigital(v,offset)-1;
					state = 2;
				}else
				{
					content+=currentChar;
					state = 0;
				}
			}else if (state==2)//2:start month field
			{
				if(isDigital(currentChar))
				{
					tempStartMonth+= currentChar;
				}
				else if(startWithYear(v,offset+1))
				{
					offset = skipToFirstDigital(v,offset)-1;
					state = 3;
				}else
					state = 0;
			}else if (state==3)//3:end year field
			{
				if(isDigital(currentChar))
				{
					tempEndYear+= currentChar;
				}
				else if(currentChar=='年'||currentChar=='-'||currentChar=='—')
				{
					if(startWithMonth(v,offset+1))
					{
						offset = skipToFirstDigital(v,offset)-1;
						state=4;
					}
					else
						state=0;
					if(currentChar=='年')
					{
						if(getFirstChar(v,offset+1)=='初'||getFirstChar(v,offset+1)=='中'||getFirstChar(v,offset+1)=='末')
							offset ++; //skip 初中未 char
					}
				}else if(currentChar=='.'||currentChar=='.')
				{
					offset = skipToFirstDigital(v,offset)-1;
					state = 4;
				}else
				{
					content+=currentChar;
					state=0;
				}
			}else if (state==4)//3:end month field
			{
				if(isDigital(currentChar))
				{
					tempEndMonth+= currentChar;
				}
				else {
					state = 0;
					if(currentChar!='月' && currentChar!=',' && currentChar!='，')
						content += currentChar;
				}
			}
			else if (state==5)//3:end month field
			{
				content += currentChar;
				if(currentChar=='》' || (currentChar=='>' &&  this.getNoneSpaceChar(v,offset,1) =='>'))
					state=0;//out from <<xxx>> quotation

			}
			offset++;
		}

		if(tempStartYear.length()>0)
		{
			ResumeSegment segment = new ResumeSegment();
			segment.setStartDate(createDate(tempStartYear,tempStartMonth));
			segment.setEndDate(createDate(tempEndYear,tempEndMonth));
			String workposition =  getWorkPosition(content);
			String studyposition =  getStudyPosition(content);
			segment.setWorkPosition(workposition);
			segment.setStudyPosition(studyposition);
			segment.setOrgnization(trimSymbols(content));
			segment.setResumeType(getResumeType(content,workposition,studyposition));
			//segment.setContent(trimSymbols(content));

			list.add(segment);
		}
		return list;
	}

	private String trimSymbols(String content) {
		int offset =0;
		char c;
		content = content.replace("学习背景","");
		content = content.replace("工作经历","");
		while(offset<content.length())
		{
			c = content.charAt(offset);
			if(isSymbols(c))
				offset++;
			else
				break;
		}
		return content.substring(offset,content.length());
	}

	private boolean isSymbols(char c)
	{
		return c<=32 || c==',' || c=='.' || c=='，' || c=='。' || c=='、'||c=='-'||c=='-'||c=='：'||c==':';
	}

	/**
	 * trim tags
	 * @param v
	 * @return
	 */
	private String trimTags(String v) {
		if(v==null || v.length()==0)
			return v;
		int offset=0;
		char c;
		int state=0;//0: in text  1:in tag
		String result="";
		while(offset<v.length())
		{
			c = v.charAt(offset);
			if(c=='<')
				state=1;
			else if(c=='>')
				state=0;
			else
			{
				if(state==0)//in text
					result+=c;

			}
			offset++;
		}
		return result;
	}

	/**
	 * check whether the string starts with a month digital
	 * the first 5 valid chars contains an one digit integer or 2 digit integer and  integer <12
	 * @param v
	 * @param offset
	 * @return
	 */
	private boolean startWithMonth(String v, int offset) {
		int count=0;
		String resultInteger="";
		boolean found= false;
		char currentChar;
		while(count<5 && offset+count<v.length())
		{
			currentChar = v.charAt(offset+count);
			if(!found)
			{
				if(isDigital(currentChar))
				{
					found = true;
					resultInteger+=currentChar;
				}
				count++;
			}	else
			{
				if(isDigital(currentChar)) {
					resultInteger += currentChar;
					count++;
				}else if(currentChar<=32 ||currentChar=='　') //skip empty chars and quanjiao space
				{
					count++;
					continue;
				}
				else
				{
					found = false;
					break;
				}
			}
		}
		if(resultInteger.length()==0)
			return false;
		else if (new Integer(resultInteger)>12)
			return false;
		else return true;
	}

	/**
	 * check whether the string starts with a year digital
	 * the first 8 chars contains a atmost 4 continues digitals ,and <=1900 the continues digitals <=2025

	 * @param v
	 * @param offset
	 * @return
	 */
	private boolean startWithYear(String v, int offset) {
		int count=0;
		String resultInteger="";
		boolean found= false;
		char currentChar;
		while(count<8 && offset+count<v.length())
		{
			currentChar = v.charAt(offset+count);
			if(!found)
			{
				if(isDigital(currentChar))
				{
					found = true;
					resultInteger+=currentChar;
				}
				count++;
			}	else
			{
				if(isDigital(currentChar)) {
					resultInteger += currentChar;
					count++;
				}else if(currentChar<=32 ||currentChar=='　') //skip empty chars and quanjiao space
				{
					count++;
					continue;
				}
				else
				{
					found = false;
					break;
				}
			}
		}
		if(resultInteger.length()==0)
			return false;
		else if (new Integer(resultInteger)<1900 || new Integer(resultInteger)>2025)
			return false;
		else return true;
	}

	private String getOrgnization(String content) {
		return content;
	}

	private int getResumeType(String content,String workPosition,String studyPositoin) {
		for(int i=0;i<ResumeSegment.RESUME_TYPE_STUDY_WORDS_1.length;i++)
		{
			if(content.indexOf(ResumeSegment.RESUME_TYPE_STUDY_WORDS_1[i])>-1)
				return ResumeSegment.RESUME_TYPE_STUDY;
		}
		for(int i=0;i<ResumeSegment.RESUME_TYPE_WORK_WORDS_1.length;i++)
		{
			if(content.indexOf(ResumeSegment.RESUME_TYPE_WORK_WORDS_1[i])>-1)
				return ResumeSegment.RESUME_TYPE_WORK;
		}


		if(studyPositoin!=null && studyPositoin.length()>0)
			return ResumeSegment.RESUME_TYPE_STUDY;

		for(int i=0;i<ResumeSegment.RESUME_TYPE_STUDY_WORDS_2.length;i++)
		{
			if(content.indexOf(ResumeSegment.RESUME_TYPE_STUDY_WORDS_2[i])>-1 && workPosition==null) //简历里有学校但是没有职务
				return ResumeSegment.RESUME_TYPE_STUDY;
		}

		return ResumeSegment.RESUME_TYPE_WORK;
	}

	private String getWorkPosition(String content) {
		for(int i=0;i<ResumeSegment.WORKING_POSITIONS.length;i++)
		{
			if(content.indexOf(ResumeSegment.WORKING_POSITIONS[i])>-1)
				return ResumeSegment.WORKING_POSITIONS[i];
		}
		return null;
	}

	private String getStudyPosition(String content) {
		for(int i=0;i<ResumeSegment.STUDY_POSITIONS.length;i++)
		{
			if(content.indexOf(ResumeSegment.STUDY_POSITIONS[i])>-1)
				return ResumeSegment.STUDY_POSITIONS[i];
		}
		return null;
	}

	/**
	 * create date object by year and month string
	 * @param yearStr
	 * @param monthStr
	 * @return
	 */
//	private Date createDate(String yearStr, String monthStr) {
//		if(yearStr==null || yearStr.length()==0)
//		{
//			return  null;
//		}
//		if(monthStr.length()==0) //by default , set the first month
//		{
//			monthStr="1";
//		}
//		Calendar calendar = Calendar.getInstance();
//		int yearInt=1900,monthInt=0;
//		if(yearStr!=null && yearStr.length()>0) {
//			try {
//				yearInt = new Integer(yearStr).intValue();
//			} catch (Exception ex) {
//				ex.printStackTrace();
//			}
//		}
//		if(monthStr!=null && monthStr.length()>0) {
//			try {
//				monthInt = new Integer(monthStr).intValue();
//			} catch (Exception ex) {
//				ex.printStackTrace();
//			}
//		}
//		calendar.set(Calendar.YEAR, yearInt);
//		calendar.set(Calendar.MONTH, monthInt-1);
//		return calendar.getTime();
//	}

	private String createDate(String yearStr, String monthStr) {
		if(yearStr==null || yearStr.length()==0)
		{
			return  null;
		}
		if(monthStr.length()==0) //by default , set the first month
		{
			return yearStr+"年";
		}else
			return yearStr+"年"+monthStr+"月";
	}


	public boolean isDigital(char c)
	{
		return c>=48 && c<=57;
	}

	/**
	 * skip to the first digital char in the v, return origial offset if digital not found in the v
	 * @param v
	 * @param offset
	 * @return
	 */
	public int skipToFirstDigital(String v,int offset)
	{
		int startoffset =offset;

		char c = v.charAt(offset++);
		while(!isDigital(c) && offset<v.length())
		{
			c=v.charAt(offset++);
		}
		if(offset==(v.length()))
			return startoffset;
		else
			return --offset;
	}

	/**
	 * chect the first non-space char is digital
	 * @param value
	 * @param offset
	 * @return
	 */
	public boolean firstCharIsDigital(String value, int offset)
	{
		char c = value.charAt(offset);
		while (c<=32)
		{
			offset++;
			c= value.charAt(offset);
		}
		return isDigital(c);
	}

	/**
	 * get first non-space char
	 * @param value
	 * @param offset
	 * @return
	 */
	public char getFirstChar(String value, int offset)
	{
		char c = value.charAt(offset);
		while (c<=32)
		{
			offset++;
			c= value.charAt(offset);
		}
		return c;
	}

	/**
	 * get the xth non-space char
	 * @param value
	 * @param offset
	 * @return
	 */
	public char getNoneSpaceChar(String value, int offset,int x)
	{
		char c = value.charAt(offset);
		int count=0;
		while (count<x)
		{
			offset++;
			c= value.charAt(offset);
			if(c>32)
			{count++;}
		}
		return c;
	}

	private static final String polNext(String str, String del, int start)
	{
		String retval;
		
		if (str==null || start>=str.length()) return ""; //$NON-NLS-1$
		
		int next = str.indexOf(del, start);
		if (next == start) // ;; or ,, : two consecutive delimiters
		{
			retval=""; //$NON-NLS-1$
		}
		else 
		if (next > start) // part of string
		{
			retval=str.substring(start, next);
		}
		else // Last field in string
		{
			retval=str.substring(start);
		}
		return retval;
	}
	
	public synchronized boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
		meta=(ResumeSplitterMeta)smi;
		data=(ResumeSplitterData)sdi;

		Object[] r=getRow();   // get row from rowset, wait for our turn, indicate busy!
		if (r==null)  // no more input to be expected...
		{
			setOutputDone();
			return false;
		}
		
		ArrayList<Object[]> outputRowDatas = splitField(r);
		for (Object[] outputRowData:outputRowDatas)
			putRow(data.outputMeta, outputRowData);

        if (checkFeedback(getLinesRead())) 
        {
        	if(log.isBasic()) logBasic(BaseMessages.getString(PKG, "ResumeSplitter.Log.LineNumber")+getLinesRead()); //$NON-NLS-1$
        }
			
		return true;
	}
	
	public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	{
		meta=(ResumeSplitterMeta)smi;
		data=(ResumeSplitterData)sdi;
		
		if (super.init(smi, sdi))
		{
		    // Add init code here.
		    return true;
		}
		return false;
	}

}