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

package org.pentaho.di.trans.steps.fieldschangeaccumulator;

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;



/*
 * Created on 30-06-2008
 *
 */

public class FieldsChangeAccumulatorMeta extends BaseStepMeta implements StepMetaInterface
{
	private static Class<?> PKG = FieldsChangeAccumulatorMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    /** by which fields to display? */
    private String  fieldName[];
    
    private String resultfieldName;
    
    private String valueField;

	private String continueValueField;
    
 //   private String increment;
    
	public FieldsChangeAccumulatorMeta()
	{
		super(); // allocate BaseStepMeta
	}
	
	public String getValueField()
	{
		return valueField;
	}
	
    /**
     * @return Returns the resultfieldName.
     */
    public String getResultFieldName()
    {
        return resultfieldName;
    }

    /**
     * @param resultName The resultfieldName to set.
     */
    public void setResultFieldName(String resultfieldName)
    {
        this.resultfieldName = resultfieldName;
    }
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String,Counter> counters)
    throws KettleXMLException
    {
        readData(stepnode);
    }

	public Object clone()
	{	
		FieldsChangeAccumulatorMeta retval = (FieldsChangeAccumulatorMeta) super.clone();

        int nrfields = fieldName.length;

        retval.allocate(nrfields);

        for (int i = 0; i < nrfields; i++)
        {
            retval.fieldName[i] = fieldName[i];
        }
		return retval;
	}
	   public void allocate(int nrfields)
	    {
	        fieldName = new String[nrfields]; 
	    }
    /**
     * @return Returns the fieldName.
     */
    public String[] getFieldName()
    {
        return fieldName;
    }
    /**
     * @param fieldName The fieldName to set.
     */
    public void setFieldName(String[] fieldName)
    {
        this.fieldName = fieldName;
    }

    public void setValueField(String vfield)
    {
    	this.valueField=vfield;
    }
    

//    public void setIncrement(String increment)
//    {
//    	this.increment=increment;
//    }
//    public String getIncrement()
//    {
//    	return increment;
//    }

	private void readData(Node stepnode)  throws KettleXMLException
	{
	  try
	    {
		  valueField = XMLHandler.getTagValue(stepnode, "start");
		  continueValueField = XMLHandler.getTagValue(stepnode, "continueValueField");
		  resultfieldName = XMLHandler.getTagValue(stepnode, "resultfieldName");
		  
		  Node fields = XMLHandler.getSubNode(stepnode, "fields");
          int nrfields = XMLHandler.countNodes(fields, "field");

          allocate(nrfields);

          for (int i = 0; i < nrfields; i++)
          {
              Node fnode = XMLHandler.getSubNodeByNr(fields, "field", i);
              fieldName[i] = XMLHandler.getTagValue(fnode, "name");
          }
	    }
      catch (Exception e)
      {
          throw new KettleXMLException("Unable to load step info from XML", e);
      }
	}
   public String getXML()
    {
        StringBuffer retval = new StringBuffer();
        retval.append("      " + XMLHandler.addTagValue("start", valueField));
        retval.append("      " + XMLHandler.addTagValue("continueValueField", continueValueField));
        retval.append("      " + XMLHandler.addTagValue("resultfieldName", resultfieldName));
        
        
        retval.append("    <fields>" + Const.CR);
        for (int i = 0; i < fieldName.length; i++)
        {
            retval.append("      <field>" + Const.CR);
            retval.append("        " + XMLHandler.addTagValue("name", fieldName[i]));
            retval.append("        </field>" + Const.CR);
        }
        retval.append("      </fields>" + Const.CR);

        return retval.toString();
    }
	public void setDefault()
	{
		resultfieldName=null;
		valueField="1";
//		increment="1";
        int nrfields = 0;

        allocate(nrfields);

        for (int i = 0; i < nrfields; i++)
        {
            fieldName[i] = "field" + i;
        }
	}


    public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String,Counter> counters)
    throws KettleException
    {
	        try
	        {
	        	valueField = rep.getStepAttributeString(id_step, "start");
	        	continueValueField = rep.getStepAttributeString(id_step, "continueValueField");
	        	resultfieldName = rep.getStepAttributeString(id_step, "resultfieldName");
	            int nrfields = rep.countNrStepAttributes(id_step, "field_name");

	            allocate(nrfields);

	            for (int i = 0; i < nrfields; i++)
	            {
	                fieldName[i] = rep.getStepAttributeString(id_step, i, "field_name");
	            }
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
	        	rep.saveStepAttribute(id_transformation, id_step, "start", valueField);
	        	rep.saveStepAttribute(id_transformation, id_step, "continueValueField", continueValueField);
	        	rep.saveStepAttribute(id_transformation, id_step, "resultfieldName", resultfieldName);
	            for (int i = 0; i < fieldName.length; i++)
	            {
	                rep.saveStepAttribute(id_transformation, id_step, i, "field_name", fieldName[i]);
	            }
	        }
	        catch (Exception e)
	        {
	            throw new KettleException("Unable to save step information to the repository for id_step=" + id_step, e);
	        }
	    }
    /**
     * need to be fixed later, length and precision should be set in a tableview,  jason 2016
     */
    public void getFields(RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space)
    {
        if (!Const.isEmpty(resultfieldName))
        {
			ValueMetaInterface v = new ValueMeta(resultfieldName, ValueMetaInterface.TYPE_NUMBER);
			v.setLength(15, 2);
			v.setOrigin(name);
			r.addValueMeta(v);
        }
    }

		
    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
    {
		CheckResult cr;
		String error_message="";
		
		if (Const.isEmpty(resultfieldName))
        {
            error_message = BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.ResultFieldMissing"); //$NON-NLS-1$
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta);
        }
        else
        {
            error_message = BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.ResultFieldOK"); //$NON-NLS-1$
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK, error_message, stepMeta);
        }
		remarks.add(cr);
		
		if (prev==null || prev.size()==0)
		{
			cr = new CheckResult(CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.NotReceivingFields"), stepMeta); //$NON-NLS-1$
			remarks.add(cr);
		}
		else
		{
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.StepRecevingData",prev.size()+""), stepMeta); //$NON-NLS-1$ //$NON-NLS-2$
			remarks.add(cr);
			
	        boolean error_found = false;
	        error_message="";
	        
            // Starting from selected fields in ...
            for (int i = 0; i < fieldName.length; i++)
            {
            	int idx = prev.indexOfValue(fieldName[i]);
                if (idx < 0)
                {
                    error_message += "\t\t" + fieldName[i] + Const.CR;
                    error_found = true;
                }
            }
            if (error_found)
            {
                error_message = BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.FieldsFound", error_message);

                cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta);
                remarks.add(cr);
            }
            else
            {
                if (fieldName.length > 0)
                {
                    cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.AllFieldsFound"), stepMeta);
                    remarks.add(cr);
                }
                else
                {
                    cr = new CheckResult(CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.NoFieldsEntered"), stepMeta);
                    remarks.add(cr);
                }
            }

		}
		
		// See if we have input streams leading to this step!
		if (input.length>0)
		{
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.StepRecevingData2"), stepMeta); //$NON-NLS-1$
			remarks.add(cr);
		}
		else
		{
			cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "FieldsChangeSequenceMeta.CheckResult.NoInputReceivedFromOtherSteps"), stepMeta); //$NON-NLS-1$
			remarks.add(cr);
		}
	}

	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans)
	{
	    return new FieldsChangeAccumulator(stepMeta, stepDataInterface, cnr, transMeta, trans);
	}
	public StepDataInterface getStepData()
	{
		return new FieldsChangeAccumulatorData();
	}

    public boolean supportsErrorHandling()
    {
        return true;
    }

	public String getContinueValueField() {
		return continueValueField;
	}

	public void setContinueValueField(String continueValueField) {
		this.continueValueField = continueValueField;
	}

}
