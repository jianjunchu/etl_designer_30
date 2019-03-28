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
import org.pentaho.di.trans.steps.resumesplitter.ResumeSplitter;
import org.pentaho.di.trans.steps.resumesplitter.ResumeSplitterData;
import org.w3c.dom.Node;


public class ResumeSplitterMeta extends BaseStepMeta implements StepMetaInterface
{
	private static Class<?> PKG = ResumeSplitterMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    /** Field to split */
    private String  splitField;

    /** new field names */
    private String  fieldName[];

    /** Field ID's to scan for */
    private String  fieldID[];

    /** flag: remove ID */
    private boolean fieldRemoveID[];

    /** type of new field */
    private int     fieldType[];

    /** formatting mask to convert value */
    private String  fieldFormat[]; 

    /** Grouping symbol */
    private String  fieldGroup[];

    /** Decimal point . or , */
    private String  fieldDecimal[];

    /** Currency symbol */
    private String  fieldCurrency[];

    /** Length of field */
    private int     fieldLength[];

    /** Precision of field */
    private int     fieldPrecision[];

    /** Replace this value with a null */
    private String  fieldNullIf[];

    /** Default value in case no value was found (ID option) */
    private String  fieldIfNull[];

    /** Perform trimming of this type on the fieldName during lookup and storage */
    private int     fieldTrimType[];

    public ResumeSplitterMeta()
    {
        super(); // allocate BaseStepMeta
    }

    public String getSplitField()
    {
        return splitField;
    }

    public void setSplitField(final String splitField)
    {
        this.splitField = splitField;
    }

    public String[] getFieldName()
    {
        return fieldName;
    }

    public void setFieldName(final String[] fieldName)
    {
        this.fieldName = fieldName;
    }

    public String[] getFieldID()
    {
        return fieldID;
    }

    public void setFieldID(final String[] fieldID)
    {
        this.fieldID = fieldID;
    }

    public boolean[] getFieldRemoveID()
    {
        return fieldRemoveID;
    }

    public void setFieldRemoveID(final boolean[] fieldRemoveID)
    {
        this.fieldRemoveID = fieldRemoveID;
    }

    public int[] getFieldType()
    {
        return fieldType;
    }

    public void setFieldType(final int[] fieldType)
    {
        this.fieldType = fieldType;
    }

    public String[] getFieldFormat()
    {
        return fieldFormat;
    }

    public void setFieldFormat(final String[] fieldFormat)
    {
        this.fieldFormat = fieldFormat;
    }

    public String[] getFieldGroup()
    {
        return fieldGroup;
    }

    public void setFieldGroup(final String[] fieldGroup)
    {
        this.fieldGroup = fieldGroup;
    }

    public String[] getFieldDecimal()
    {
        return fieldDecimal;
    }

    public void setFieldDecimal(final String[] fieldDecimal)
    {
        this.fieldDecimal = fieldDecimal;
    }

    public String[] getFieldCurrency()
    {
        return fieldCurrency;
    }

    public void setFieldCurrency(final String[] fieldCurrency)
    {
        this.fieldCurrency = fieldCurrency;
    }

    public int[] getFieldLength()
    {
        return fieldLength;
    }

    public void setFieldLength(final int[] fieldLength)
    {
        this.fieldLength = fieldLength;
    }

    public int[] getFieldPrecision()
    {
        return fieldPrecision;
    }

    public void setFieldPrecision(final int[] fieldPrecision)
    {
        this.fieldPrecision = fieldPrecision;
    }

    public String[] getFieldNullIf()
    {
        return fieldNullIf;
    }

    public void setFieldNullIf(final String[] fieldNullIf)
    {
        this.fieldNullIf = fieldNullIf;
    }

    public String[] getFieldIfNull()
    {
        return fieldIfNull;
    }

    public void setFieldIfNull(final String[] fieldIfNull)
    {
        this.fieldIfNull = fieldIfNull;
    }

    public int[] getFieldTrimType()
    {
        return fieldTrimType;
    }

    public void setFieldTrimType(final int[] fieldTrimType)
    {
        this.fieldTrimType = fieldTrimType;
    }



    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String,Counter> counters)
        throws KettleXMLException
        {
            readData(stepnode);
        }

    public void allocate(int nrfields)
    {
        fieldName = new String[nrfields];
        fieldID        = new String[nrfields];
        fieldRemoveID = new boolean[nrfields];
        fieldType       = new int[nrfields];
        fieldFormat     = new String[nrfields];
        fieldGroup      = new String[nrfields];
        fieldDecimal    = new String[nrfields];
        fieldCurrency   = new String[nrfields];
        fieldLength     = new int[nrfields];
        fieldPrecision  = new int[nrfields];
        fieldNullIf = new String[nrfields];
        fieldIfNull = new String[nrfields];
        fieldTrimType = new int[nrfields];
    }

    public Object clone()
    {
        ResumeSplitterMeta retval = (ResumeSplitterMeta)super.clone();

        final int nrfields = fieldName.length;

        retval.allocate(nrfields);

        for (int i=0;i<nrfields;i++)
        {
            retval.fieldName[i] = fieldName[i];
            retval.fieldID       [i] = fieldID[i]; 
            retval.fieldRemoveID[i] = fieldRemoveID[i];
            retval.fieldType[i] = fieldType[i];
            retval.fieldLength[i] = fieldLength[i];
            retval.fieldPrecision[i] = fieldPrecision[i];
            retval.fieldFormat    [i] = fieldFormat[i];
            retval.fieldGroup     [i] = fieldGroup[i];
            retval.fieldDecimal   [i] = fieldDecimal[i];
            retval.fieldCurrency  [i] = fieldCurrency[i];
            retval.fieldNullIf[i] = fieldNullIf[i];
            retval.fieldIfNull[i] = fieldIfNull[i];
            retval.fieldTrimType[i] = fieldTrimType[i];
        }

        return retval;
    }

    private void readData(Node stepnode)
        throws KettleXMLException
        {
            try
            {
                splitField = XMLHandler.getTagValue(stepnode, "splitfield"); //$NON-NLS-1$

                final Node fields = XMLHandler.getSubNode(stepnode, "fields"); //$NON-NLS-1$
                final int nrfields = XMLHandler.countNodes(fields, "field"); //$NON-NLS-1$

                allocate(nrfields);

                for (int i=0;i<nrfields;i++)
                {
                    final Node fnode = XMLHandler.getSubNodeByNr(fields, "field", i); //$NON-NLS-1$

                    fieldName[i] = XMLHandler.getTagValue(fnode, "name"); //$NON-NLS-1$
                    fieldID       [i]  = XMLHandler.getTagValue(fnode, "id"); //$NON-NLS-1$
                    final String sidrem = XMLHandler.getTagValue(fnode, "idrem"); //$NON-NLS-1$
                    final String stype = XMLHandler.getTagValue(fnode, "type"); //$NON-NLS-1$
                    fieldFormat    [i]  = XMLHandler.getTagValue(fnode, "format"); //$NON-NLS-1$
                    fieldGroup     [i]  = XMLHandler.getTagValue(fnode, "group"); //$NON-NLS-1$
                    fieldDecimal   [i]  = XMLHandler.getTagValue(fnode, "decimal"); //$NON-NLS-1$
                    fieldCurrency  [i]  = XMLHandler.getTagValue(fnode, "currency"); //$NON-NLS-1$
                    final String slen = XMLHandler.getTagValue(fnode, "length"); //$NON-NLS-1$
                    final String sprc = XMLHandler.getTagValue(fnode, "precision"); //$NON-NLS-1$
                    fieldNullIf[i] = XMLHandler.getTagValue(fnode, "nullif"); //$NON-NLS-1$
                    fieldIfNull[i] = XMLHandler.getTagValue(fnode, "ifnull"); //$NON-NLS-1$
                    final String trim = XMLHandler.getTagValue(fnode, "trimtype"); //$NON-NLS-1$

                    fieldRemoveID[i] = "Y".equalsIgnoreCase(sidrem); //$NON-NLS-1$
                    fieldType[i]  = ValueMeta.getType(stype);
                    fieldLength   [i]=Const.toInt(slen, -1); 
                    fieldPrecision[i]=Const.toInt(sprc, -1); 
                    fieldTrimType[i] = ValueMeta.getTrimTypeByCode(trim);
                }
            }
            catch(Exception e)
            {
                throw new KettleXMLException(BaseMessages.getString(PKG, "ResumeSplitterMeta.Exception.UnableToLoadStepInfoFromXML"), e); //$NON-NLS-1$
            }
        }

    public void setDefault()
    {
        splitField = ""; //$NON-NLS-1$
        allocate(0);
    }

//    public void getFields(RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space)
//    {
//        // Remove the field to split
//        int idx = r.indexOfValue(splitField);
//        if (idx<0) //not found
//        {
//            throw new RuntimeException(BaseMessages.getString(PKG, "ResumeSplitter.Log.CouldNotFindFieldToSplit",splitField)); //$NON-NLS-1$ //$NON-NLS-2$
//        }
//
//        // Add the new fields at the place of the index --> replace!
//        for (int i = 0; i < fieldName.length; i++)
//        {
//            final ValueMetaInterface v = new ValueMeta(fieldName[i], fieldType[i]);
//            v.setLength(fieldLength[i], fieldPrecision[i]);
//            v.setOrigin(name);
//            v.setConversionMask(fieldFormat[i]);
//            v.setDecimalSymbol(fieldDecimal[i]);
//            v.setGroupingSymbol(fieldGroup[i]);
//            v.setCurrencySymbol(fieldCurrency[i]);
//            v.setTrimType(fieldTrimType[i]);
//            // TODO when implemented in UI
//            // v.setDateFormatLenient(dateFormatLenient);
//            // TODO when implemented in UI
//            // v.setDateFormatLocale(dateFormatLocale);
//            if(i==0 && idx>=0)
//            {
//                //the first valueMeta (splitField) will be replaced
//                r.setValueMeta(idx, v);
//            }
//            else
//            {
//                //other valueMeta will be added
//                if (idx>=r.size()) r.addValueMeta(v);
//                r.addValueMeta(idx+i, v);
//            }
//        }
//    }

    public void getFields(RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space)
    {
        // Remove the field to split
        int length = r.size();

        final ValueMetaInterface v1 = new ValueMeta("开始日期", ValueMetaInterface.TYPE_STRING);
        v1.setLength(0, 0);
        v1.setOrigin(name);
//        v1.setConversionMask("YYYY-mm");
//        v1.setDecimalSymbol(".");
//        v1.setGroupingSymbol(",");
//        v1.setCurrencySymbol("$");
        v1.setTrimType(ValueMetaInterface.TRIM_TYPE_BOTH);
        r.addValueMeta(length+0, v1);

        final ValueMetaInterface v2 = new ValueMeta("结束日期", ValueMetaInterface.TYPE_STRING);
        v2.setLength(0, 0);
        v2.setOrigin(name);
//        v2.setConversionMask("YYYY-mm");
//        v2.setDecimalSymbol(".");
//        v2.setGroupingSymbol(",");
//        v2.setCurrencySymbol("$");
        v2.setTrimType(ValueMetaInterface.TRIM_TYPE_BOTH);
        r.addValueMeta(length+1, v2);

        final ValueMetaInterface v3 = new ValueMeta("简历类型", ValueMetaInterface.TYPE_STRING);
        v3.setLength(10, 0);
        v3.setOrigin(name);
        r.addValueMeta(length+2, v3);

        final ValueMetaInterface v4 = new ValueMeta("单位", ValueMetaInterface.TYPE_STRING);
        v4.setLength(1000, 0);
        v4.setOrigin(name);
        r.addValueMeta(length+3, v4);

        final ValueMetaInterface v5 = new ValueMeta("学位", ValueMetaInterface.TYPE_STRING);
        v5.setLength(50, 0);
        v5.setOrigin(name);
        r.addValueMeta(length+4, v5);

        final ValueMetaInterface v6 = new ValueMeta("职务", ValueMetaInterface.TYPE_STRING);
        v6.setLength(50, 0);
        v6.setOrigin(name);
         r.addValueMeta(length+5, v6);

    }
    public String getXML()
    {
        final StringBuilder retval = new StringBuilder(500);

        retval.append("   ").append(XMLHandler.addTagValue("splitfield", splitField)); //$NON-NLS-1$ //$NON-NLS-2$

//        retval.append("    <fields>"); //$NON-NLS-1$
//        for (int i = 0; i < fieldName.length; i++)
//        {
//            retval.append("      <field>"); //$NON-NLS-1$
//            retval.append("        ").append(XMLHandler.addTagValue("name", fieldName[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("id",        fieldID[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("idrem", fieldRemoveID[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("type",      ValueMeta.getTypeDesc(fieldType[i]))); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("format",    fieldFormat[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("group",     fieldGroup[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("decimal",   fieldDecimal[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("length",    fieldLength[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("precision", fieldPrecision[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("nullif", fieldNullIf[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("ifnull", fieldIfNull[i])); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("        ").append(XMLHandler.addTagValue("trimtype", ValueMeta.getTrimTypeCode(fieldTrimType[i]))); //$NON-NLS-1$ //$NON-NLS-2$
//            retval.append("      </field>"); //$NON-NLS-1$
//        }
//        retval.append("    </fields>"); //$NON-NLS-1$

        return retval.toString();
    }

    public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String,Counter> counters)
        throws KettleException
        {
            try
            {
                splitField  = rep.getStepAttributeString(id_step, "splitfield"); //$NON-NLS-1$

                int nrfields = rep.countNrStepAttributes(id_step, "field_name"); //$NON-NLS-1$

//                allocate(nrfields);
//
//                for (int i=0;i<nrfields;i++)
//                {
//                    fieldName[i] = rep.getStepAttributeString(id_step, i, "field_name"); //$NON-NLS-1$
//                    fieldID[i]         =       rep.getStepAttributeString (id_step, i, "field_id"); //$NON-NLS-1$
//                    fieldRemoveID[i] = rep.getStepAttributeBoolean(id_step, i, "field_idrem"); //$NON-NLS-1$
//                    fieldType[i]        =  ValueMeta.getType( rep.getStepAttributeString (id_step, i, "field_type") ); //$NON-NLS-1$
//                    fieldFormat[i]      =       rep.getStepAttributeString (id_step, i, "field_format"); //$NON-NLS-1$
//                    fieldGroup[i]       =       rep.getStepAttributeString (id_step, i, "field_group"); //$NON-NLS-1$
//                    fieldDecimal[i]     =       rep.getStepAttributeString (id_step, i, "field_decimal"); //$NON-NLS-1$
//                    fieldLength[i]      =  (int)rep.getStepAttributeInteger(id_step, i, "field_length"); //$NON-NLS-1$
//                    fieldPrecision[i]   =  (int)rep.getStepAttributeInteger(id_step, i, "field_precision"); //$NON-NLS-1$
//                    fieldNullIf[i] = rep.getStepAttributeString(id_step, i, "field_nullif"); //$NON-NLS-1$
//                    fieldIfNull[i] = rep.getStepAttributeString(id_step, i, "field_ifnull"); //$NON-NLS-1$
//                    fieldTrimType[i] =  ValueMeta.getTrimTypeByCode(rep.getStepAttributeString(id_step, i, "field_trimtype")); //$NON-NLS-1$
//                }
            }
            catch(Exception e)
            {
                throw new KettleException(BaseMessages.getString(PKG, "ResumeSplitterMeta.Exception.UnexpectedErrorInReadingStepInfo"), e); //$NON-NLS-1$
            }
        }

    public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step)
        throws KettleException
        {
            try
            {
                rep.saveStepAttribute(id_transformation, id_step, "splitfield", splitField); //$NON-NLS-1$

//                for (int i = 0; i < fieldName.length; i++)
//                {
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_name", fieldName[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_id",        fieldID[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_idrem", fieldRemoveID[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_type",      ValueMeta.getTypeDesc(fieldType[i])); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_format",    fieldFormat[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_group",     fieldGroup[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_decimal",   fieldDecimal[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_length",    fieldLength[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_precision", fieldPrecision[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_nullif", fieldNullIf[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_ifnull", fieldIfNull[i]); //$NON-NLS-1$
//                    rep.saveStepAttribute(id_transformation, id_step, i, "field_trimtype", ValueMeta.getTrimTypeCode(fieldTrimType[i])); //$NON-NLS-1$
//                }
            }
            catch(Exception e)
            {
                throw new KettleException(BaseMessages.getString(PKG, "ResumeSplitterMeta.Exception.UnalbeToSaveStepInfoToRepository")+id_step, e); //$NON-NLS-1$
            }
        }

    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String input[], String output[], RowMetaInterface info)
    {
        String error_message=""; //$NON-NLS-1$
        CheckResult cr;

        // Look up fields in the input stream <prev>
        if (prev!=null && prev.size()>0)
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "ResumeSplitterMeta.CheckResult.StepReceivingFields",prev.size()+""), stepMeta); //$NON-NLS-1$ //$NON-NLS-2$
            remarks.add(cr);

            error_message = ""; //$NON-NLS-1$

            int i = prev.indexOfValue(splitField);
            if (i<0)
            {
                error_message=BaseMessages.getString(PKG, "ResumeSplitterMeta.CheckResult.SplitedFieldNotPresentInInputStream",splitField); //$NON-NLS-1$ //$NON-NLS-2$
                cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta);
                remarks.add(cr);
            }
            else
            {
                cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "ResumeSplitterMeta.CheckResult.SplitedFieldFoundInInputStream",splitField), stepMeta); //$NON-NLS-1$ //$NON-NLS-2$
                remarks.add(cr);
            }
        }
        else
        {
            error_message=BaseMessages.getString(PKG, "ResumeSplitterMeta.CheckResult.CouldNotReadFieldsFromPreviousStep")+Const.CR; //$NON-NLS-1$
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, error_message, stepMeta);
            remarks.add(cr);
        }

        // See if we have input streams leading to this step!
        if (input.length>0)
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_OK, BaseMessages.getString(PKG, "ResumeSplitterMeta.CheckResult.StepReceivingInfoFromOtherStep"), stepMeta); //$NON-NLS-1$
            remarks.add(cr);
        }
        else
        {
            cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(PKG, "ResumeSplitterMeta.CheckResult.NoInputReceivedFromOtherStep"), stepMeta); //$NON-NLS-1$
            remarks.add(cr);
        }
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans)
    {
        return new ResumeSplitter(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    public StepDataInterface getStepData()
    {
        return new ResumeSplitterData();
    }

}
