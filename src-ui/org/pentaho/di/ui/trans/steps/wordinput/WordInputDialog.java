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

package org.pentaho.di.ui.trans.steps.wordinput;

import com.aspose.words.*;
import com.aspose.words.Table;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.provider.local.LocalFile;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringEvaluationResult;
import org.pentaho.di.core.util.StringEvaluator;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.wordinput.WordInput;
import org.pentaho.di.trans.steps.wordinput.WordInputMeta;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.*;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class WordInputDialog extends BaseStepDialog implements StepDialogInterface
{
	private static Class<?> PKG = WordInput.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private WordInputMeta inputMeta;

	private TextVar      wFilename;
	private CCombo       wFilenameField;
	private Button       wbbFilename; // Browse for a file
	private Button       wExtractSpecifiedTable;
	private Button       wIncludeFilename;
	private TextVar      wRowNumField;
	private Button       wbStartRowIndex;
	private TextVar      wStartRowIndex;
	private TextVar 	 wTableNr;
	private Button       wHeaderPresent;
	private FormData	 fdAddResult;
	private FormData	 fdlAddResult;
	private TableView    wFields;
	private Label wlAddResult;
  	private Button wAddResult;
	private boolean isReceivingInput;
	private boolean gotEncodings = false;

	public WordInputDialog(Shell parent, Object in, TransMeta tr, String sname)
	{
		super(parent, (BaseStepMeta)in, tr, sname);
		inputMeta=(WordInputMeta)in;
	}

	public String open()
	{
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
 		props.setLook(shell);
 		setShellImage(shell, inputMeta);
        
		ModifyListener lsMod = new ModifyListener() 
		{
			public void modifyText(ModifyEvent e) 
			{
				inputMeta.setChanged();
			}
		};
		changed = inputMeta.hasChanged();

		FormLayout formLayout = new FormLayout ();
		formLayout.marginWidth  = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "WordInputDialog.Shell.Title")); //$NON-NLS-1$
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Step name line
		//
		wlStepname=new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "WordInputDialog.Stepname.Label")); //$NON-NLS-1$
 		props.setLook(wlStepname);
		fdlStepname=new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right= new FormAttachment(middle, -margin);
		fdlStepname.top  = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname=new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top  = new FormAttachment(0, margin);
		fdStepname.right= new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);
		Control lastControl = wStepname;
		
		
		// See if the step receives input.  If so, we don't ask for the filename, but for the filename field.
		//
		isReceivingInput = transMeta.findNrPrevSteps(stepMeta)>0;
		if (isReceivingInput) {
			
			RowMetaInterface previousFields;
			try {
				previousFields = transMeta.getPrevStepFields(stepMeta);
			}
			catch(KettleStepException e) {
				new ErrorDialog(shell, BaseMessages.getString(PKG, "WordInputDialog.ErrorDialog.UnableToGetInputFields.Title"), BaseMessages.getString(PKG, "WordInputDialog.ErrorDialog.UnableToGetInputFields.Message"), e);
				previousFields = new RowMeta();
			}
			
			// The filename field ...
			//
			Label wlFilename = new Label(shell, SWT.RIGHT);
			wlFilename.setText(BaseMessages.getString(PKG, inputMeta.getDescription("FILENAME_FIELD"))); //$NON-NLS-1$
	 		props.setLook(wlFilename);
			FormData fdlFilename = new FormData();
			fdlFilename.top  = new FormAttachment(lastControl, margin);
			fdlFilename.left = new FormAttachment(0, 0);
			fdlFilename.right= new FormAttachment(middle, -margin);
			wlFilename.setLayoutData(fdlFilename);
			wFilenameField=new CCombo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
			wFilenameField.setItems(previousFields.getFieldNames());
	 		props.setLook(wFilenameField);
	 		wFilenameField.addModifyListener(lsMod);
			FormData fdFilename = new FormData();
			fdFilename.top  = new FormAttachment(lastControl, margin);
			fdFilename.left = new FormAttachment(middle, 0);
			fdFilename.right= new FormAttachment(100, 0);
			wFilenameField.setLayoutData(fdFilename);
			lastControl = wFilenameField;
			
			// Checkbox to include the filename in the output...
			//
			Label wlIncludeFilename = new Label(shell, SWT.RIGHT);
			wlIncludeFilename.setText(BaseMessages.getString(PKG, inputMeta.getDescription("INCLUDE_FILENAME"))); //$NON-NLS-1$
	 		props.setLook(wlIncludeFilename);
			FormData fdlIncludeFilename = new FormData();
			fdlIncludeFilename.top  = new FormAttachment(lastControl, margin);
			fdlIncludeFilename.left = new FormAttachment(0, 0);
			fdlIncludeFilename.right= new FormAttachment(middle, -margin);
			wlIncludeFilename.setLayoutData(fdlIncludeFilename);
			wIncludeFilename=new Button(shell, SWT.CHECK);
	 		props.setLook(wIncludeFilename);
	 		wFilenameField.addModifyListener(lsMod);
			FormData fdIncludeFilename = new FormData();
			fdIncludeFilename.top  = new FormAttachment(lastControl, margin);
			fdIncludeFilename.left = new FormAttachment(middle, 0);
			fdIncludeFilename.right= new FormAttachment(100, 0);
			wIncludeFilename.setLayoutData(fdIncludeFilename);
			lastControl = wIncludeFilename;
		}
		else {
			
			// Filename...
			//
			// The filename browse button
			//
	        wbbFilename=new Button(shell, SWT.PUSH| SWT.CENTER);
	        props.setLook(wbbFilename);
	        wbbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
	        wbbFilename.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
	        FormData fdbFilename = new FormData();
	        fdbFilename.top  = new FormAttachment(lastControl, margin);
	        fdbFilename.right= new FormAttachment(100, 0);
	        wbbFilename.setLayoutData(fdbFilename);
	
	        // The field itself...
	        //
			Label wlFilename = new Label(shell, SWT.RIGHT);
			wlFilename.setText(BaseMessages.getString(PKG, inputMeta.getDescription("FILENAME"))); //$NON-NLS-1$
	 		props.setLook(wlFilename);
			FormData fdlFilename = new FormData();
			fdlFilename.top  = new FormAttachment(lastControl, margin);
			fdlFilename.left = new FormAttachment(0, 0);
			fdlFilename.right= new FormAttachment(middle, -margin);
			wlFilename.setLayoutData(fdlFilename);
			wFilename=new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
	 		props.setLook(wFilename);
			wFilename.addModifyListener(lsMod);
			FormData fdFilename = new FormData();
			fdFilename.top  = new FormAttachment(lastControl, margin);
			fdFilename.left = new FormAttachment(middle, 0);
			fdFilename.right= new FormAttachment(wbbFilename, -margin);
			wFilename.setLayoutData(fdFilename);
			lastControl = wFilename;
		}

		//wExtractSpecifiedTable
		Label wlExtractSpecTable = new Label(shell, SWT.RIGHT);
		wlExtractSpecTable.setText(BaseMessages.getString(PKG, inputMeta.getDescription("EXTRACT_SPEC_TABLE"))); //$NON-NLS-1$
		props.setLook(wlExtractSpecTable);
		FormData fdlExtractSpecTable = new FormData();
		fdlExtractSpecTable.top  = new FormAttachment(lastControl, margin);
		fdlExtractSpecTable.left = new FormAttachment(0, 0);
		fdlExtractSpecTable.right= new FormAttachment(middle, -margin);
		wlExtractSpecTable.setLayoutData(fdlExtractSpecTable);
		wExtractSpecifiedTable = new Button(shell, SWT.CHECK);
		props.setLook(wExtractSpecifiedTable);
		//wExtractSpecifiedTable.addSelectionListener(lsDef);
		FormData fdExtractSpecTable = new FormData();
		fdExtractSpecTable.top  = new FormAttachment(lastControl, margin);
		fdExtractSpecTable.left = new FormAttachment(middle, 0);
		fdExtractSpecTable.right= new FormAttachment(100, 0);
		wExtractSpecifiedTable.setLayoutData(fdExtractSpecTable);
		lastControl = wExtractSpecifiedTable;

		// tableNr
		//
		Label wlBufferSize = new Label(shell, SWT.RIGHT);
		wlBufferSize.setText(BaseMessages.getString(PKG, inputMeta.getDescription("TABLE_NR"))); //$NON-NLS-1$
 		props.setLook(wlBufferSize);
		FormData fdlBufferSize = new FormData();
		fdlBufferSize.top  = new FormAttachment(lastControl, margin);
		fdlBufferSize.left = new FormAttachment(0, 0);
		fdlBufferSize.right= new FormAttachment(middle, -margin);
		wlBufferSize.setLayoutData(fdlBufferSize);
		wTableNr = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wTableNr);
		wTableNr.addModifyListener(lsMod);
		FormData fdBufferSize = new FormData();
		fdBufferSize.top  = new FormAttachment(lastControl, margin);
		fdBufferSize.left = new FormAttachment(middle, 0);
		fdBufferSize.right= new FormAttachment(100, 0);
		wTableNr.setLayoutData(fdBufferSize);
		lastControl = wTableNr;

		// row index
		Label wlDelimiter = new Label(shell, SWT.RIGHT);
		wlDelimiter.setText(BaseMessages.getString(PKG, inputMeta.getDescription("START_ROW_INDEX"))); //$NON-NLS-1$
		props.setLook(wlDelimiter);
		FormData fdlDelimiter = new FormData();
		fdlDelimiter.top  = new FormAttachment(lastControl, margin);
		fdlDelimiter.left = new FormAttachment(0, 0);
		fdlDelimiter.right= new FormAttachment(middle, -margin);
		wlDelimiter.setLayoutData(fdlDelimiter);
		wbStartRowIndex =new Button(shell, SWT.PUSH| SWT.CENTER);
		props.setLook(wbStartRowIndex);
		wbStartRowIndex.setText(BaseMessages.getString(PKG, "WordInputDialog.Delimiter.Button"));
		FormData fdbDelimiter=new FormData();
		fdbDelimiter.top  = new FormAttachment(lastControl, margin);
		fdbDelimiter.right= new FormAttachment(100, 0);
		wbStartRowIndex.setLayoutData(fdbDelimiter);
		wStartRowIndex =new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wStartRowIndex);
		wStartRowIndex.addModifyListener(lsMod);
		FormData fdDelimiter = new FormData();
		fdDelimiter.top  = new FormAttachment(lastControl, margin);
		fdDelimiter.left = new FormAttachment(middle, 0);
		fdDelimiter.right= new FormAttachment(wbStartRowIndex, -margin);
		wStartRowIndex.setLayoutData(fdDelimiter);
		lastControl = wStartRowIndex;

		// header row?
		//
		Label wlHeaderPresent = new Label(shell, SWT.RIGHT);
		wlHeaderPresent.setText(BaseMessages.getString(PKG, inputMeta.getDescription("HEADER_PRESENT"))); //$NON-NLS-1$
 		props.setLook(wlHeaderPresent);
		FormData fdlHeaderPresent = new FormData();
		fdlHeaderPresent.top  = new FormAttachment(lastControl, margin);
		fdlHeaderPresent.left = new FormAttachment(0, 0);
		fdlHeaderPresent.right= new FormAttachment(middle, -margin);
		wlHeaderPresent.setLayoutData(fdlHeaderPresent);
		wHeaderPresent = new Button(shell, SWT.CHECK);
 		props.setLook(wHeaderPresent);
		FormData fdHeaderPresent = new FormData();
		fdHeaderPresent.top  = new FormAttachment(lastControl, margin);
		fdHeaderPresent.left = new FormAttachment(middle, 0);
		fdHeaderPresent.right= new FormAttachment(100, 0);
		wHeaderPresent.setLayoutData(fdHeaderPresent);
		lastControl = wHeaderPresent;
		
		wlAddResult=new Label(shell, SWT.RIGHT);
		wlAddResult.setText(BaseMessages.getString(PKG, inputMeta.getDescription("ADD_FILENAME_RESULT")));
 		props.setLook(wlAddResult);
		fdlAddResult=new FormData();
		fdlAddResult.left = new FormAttachment(0, 0);
		fdlAddResult.top  = new FormAttachment(wHeaderPresent, margin);
		fdlAddResult.right= new FormAttachment(middle, -margin);
		wlAddResult.setLayoutData(fdlAddResult);
		wAddResult=new Button(shell, SWT.CHECK );
 		props.setLook(wAddResult);
		wAddResult.setToolTipText(BaseMessages.getString(PKG, inputMeta.getTooltip("ADD_FILENAME_RESULT")));
		fdAddResult=new FormData();
		fdAddResult.left = new FormAttachment(middle, 0);
		fdAddResult.top  = new FormAttachment(wHeaderPresent, margin);
		wAddResult.setLayoutData(fdAddResult);
		lastControl = wAddResult;
		
        // The field itself...
        //
		Label wlRowNumField = new Label(shell, SWT.RIGHT);
		wlRowNumField.setText(BaseMessages.getString(PKG, inputMeta.getDescription("ROW_NUM_FIELD"))); //$NON-NLS-1$
 		props.setLook(wlRowNumField);
		FormData fdlRowNumField = new FormData();
		fdlRowNumField.top  = new FormAttachment(lastControl, margin);
		fdlRowNumField.left = new FormAttachment(0, 0);
		fdlRowNumField.right= new FormAttachment(middle, -margin);
		wlRowNumField.setLayoutData(fdlRowNumField);
		wRowNumField=new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wRowNumField);
		wRowNumField.addModifyListener(lsMod);
		FormData fdRowNumField = new FormData();
		fdRowNumField.top  = new FormAttachment(lastControl, margin);
		fdRowNumField.left = new FormAttachment(middle, 0);
		fdRowNumField.right= new FormAttachment(100, 0);
		wRowNumField.setLayoutData(fdRowNumField);
		lastControl = wRowNumField;


		// Some buttons first, so that the dialog scales nicely...
		//
		wOK=new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$
		wCancel=new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$
		wPreview=new Button(shell, SWT.PUSH);
		wPreview.setText(BaseMessages.getString(PKG, "System.Button.Preview")); //$NON-NLS-1$
		wPreview.setEnabled(!isReceivingInput);
		wGet=new Button(shell, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields")); //$NON-NLS-1$
		wGet.setEnabled(!isReceivingInput);

		setButtonPositions(new Button[] { wOK, wGet, wPreview, wCancel }, margin, null);


		// Fields
        ColumnInfo[] colinf=new ColumnInfo[]
            {
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_NAME")),       ColumnInfo.COLUMN_TYPE_TEXT,    false),
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_TYPE")),       ColumnInfo.COLUMN_TYPE_CCOMBO,  ValueMeta.getTypes(), true ),
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_FORMAT")),     ColumnInfo.COLUMN_TYPE_FORMAT,  2),
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_LENGTH")),     ColumnInfo.COLUMN_TYPE_TEXT,    false),
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_PRECISION")),  ColumnInfo.COLUMN_TYPE_TEXT,    false),
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_CURRENCY")),   ColumnInfo.COLUMN_TYPE_TEXT,    false),
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_DECIMAL")),    ColumnInfo.COLUMN_TYPE_TEXT,    false),
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_GROUP")),      ColumnInfo.COLUMN_TYPE_TEXT,    false),
             new ColumnInfo(BaseMessages.getString(PKG, inputMeta.getDescription("FIELD_TRIM_TYPE")),  ColumnInfo.COLUMN_TYPE_CCOMBO,  ValueMeta.trimTypeDesc),
            };
        
        colinf[2].setComboValuesSelectionListener(new ComboValuesSelectionListener() {
    		
			public String[] getComboValues(TableItem tableItem, int rowNr, int colNr) {
				String[] comboValues = new String[] { };
				int type = ValueMeta.getType( tableItem.getText(colNr-1) );
				switch(type) {
				case ValueMetaInterface.TYPE_DATE: comboValues = Const.getDateFormats(); break;
				case ValueMetaInterface.TYPE_INTEGER: 
				case ValueMetaInterface.TYPE_BIGNUMBER:
				case ValueMetaInterface.TYPE_NUMBER: comboValues = Const.getNumberFormats(); break;
				default: break;
				}
				return comboValues;
			}
		
		});

        
        wFields=new TableView(transMeta, shell, 
                              SWT.FULL_SELECTION | SWT.MULTI, 
                              colinf, 
                              1,  
                              lsMod,
                              props
                              );

        FormData fdFields = new FormData();
        fdFields.top   = new FormAttachment(lastControl, margin*2);
        fdFields.bottom= new FormAttachment(wOK, -margin*2);
        fdFields.left  = new FormAttachment(0, 0);
        fdFields.right = new FormAttachment(100, 0);
        wFields.setLayoutData(fdFields);
        
		// Add listeners
		lsCancel   = new Listener() { public void handleEvent(Event e) { cancel(); } };
		lsOK       = new Listener() { public void handleEvent(Event e) { ok();     } };
		lsPreview  = new Listener() { public void handleEvent(Event e) { preview(); } };
		lsGet      = new Listener() { public void handleEvent(Event e) { getCSV(); } };

		wCancel.addListener (SWT.Selection, lsCancel );
		wOK.addListener     (SWT.Selection, lsOK     );
		wPreview.addListener(SWT.Selection, lsPreview);
		wGet.addListener    (SWT.Selection, lsGet    );
		
		lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };
		
		wStepname.addSelectionListener( lsDef );
		if (wFilename!=null) wFilename.addSelectionListener( lsDef );
		if (wFilenameField!=null) wFilenameField.addSelectionListener( lsDef );
		wStartRowIndex.addSelectionListener( lsDef );
		wTableNr.addSelectionListener( lsDef );
		wRowNumField.addSelectionListener( lsDef );
		
		// Allow the insertion of tabs as separator...
		wbStartRowIndex.addSelectionListener(new SelectionAdapter()
			{
				public void widgetSelected(SelectionEvent se) 
				{
					Text t = wStartRowIndex.getTextWidget();
					if ( t != null )
					    t.insert("\t");
				}
			}
		);

		if (wbbFilename!=null) {
			// Listen to the browse button next to the file name
			wbbFilename.addSelectionListener(
					new SelectionAdapter()
					{
						public void widgetSelected(SelectionEvent e) 
						{
							FileDialog dialog = new FileDialog(shell, SWT.OPEN);
							dialog.setFilterExtensions(new String[] {"*.txt;*.csv", "*.csv", "*.txt", "*"});
							if (wFilename.getText()!=null)
							{
								String fname = transMeta.environmentSubstitute(wFilename.getText());
								dialog.setFileName( fname );
							}
							
							dialog.setFilterNames(new String[] {BaseMessages.getString(PKG, "System.FileType.CSVFiles")+", "+BaseMessages.getString(PKG, "System.FileType.TextFiles"), BaseMessages.getString(PKG, "System.FileType.CSVFiles"), BaseMessages.getString(PKG, "System.FileType.TextFiles"), BaseMessages.getString(PKG, "System.FileType.AllFiles")});
							
							if (dialog.open()!=null)
							{
								String str = dialog.getFilterPath()+System.getProperty("file.separator")+dialog.getFileName();
								wFilename.setText(str);
							}
						}
					}
				);
		}

		
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(	new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } } );


		// Set the shell size, based upon previous time...
		setSize();

		getData();
		inputMeta.setChanged(changed);
		//checkPriviledges();
		shell.open();
		while (!shell.isDisposed())
		{
				if (!display.readAndDispatch()) display.sleep();
		}
		return stepname;
	}

	
	public void getData()
	{
		getData(inputMeta, true);
	}
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */ 
	public void getData(WordInputMeta inputMeta, boolean copyStepname)
	{
	  if (copyStepname) {
		wStepname.setText(stepname);
	  }
		if (isReceivingInput) {
			wFilenameField.setText(Const.NVL(inputMeta.getFilenameField(), ""));
			wIncludeFilename.setSelection(inputMeta.isIncludingFilename());
		} else {
			wFilename.setText(Const.NVL(inputMeta.getFilename(), ""));
		}
		wStartRowIndex.setText(new Integer(inputMeta.getStartRowIndex()).toString());
		wTableNr.setText(Const.NVL(inputMeta.getTableNr(), ""));
		wHeaderPresent.setSelection(inputMeta.isHeaderPresent());
		wRowNumField.setText(Const.NVL(inputMeta.getRowNumField(), ""));
		wAddResult.setSelection(inputMeta.isAddResultFile());
		wExtractSpecifiedTable.setSelection(inputMeta.isExtractSpecifiedTable());
		for (int i=0;i<inputMeta.getInputFields().length;i++) {
			TextFileInputField field = inputMeta.getInputFields()[i];
			
			TableItem item = new TableItem(wFields.table, SWT.NONE);
			int colnr=1;
			item.setText(colnr++, Const.NVL(field.getName(), ""));
			item.setText(colnr++, ValueMeta.getTypeDesc(field.getType()));
			item.setText(colnr++, Const.NVL(field.getFormat(), ""));
			item.setText(colnr++, field.getLength()>=0?Integer.toString(field.getLength()):"") ;
			item.setText(colnr++, field.getPrecision()>=0?Integer.toString(field.getPrecision()):"") ;
			item.setText(colnr++, Const.NVL(field.getCurrencySymbol(), ""));
			item.setText(colnr++, Const.NVL(field.getDecimalSymbol(), ""));
			item.setText(colnr++, Const.NVL(field.getGroupSymbol(), ""));
			item.setText(colnr++, Const.NVL(field.getTrimTypeDesc(), ""));
		}
		wFields.removeEmptyRows();
		wFields.setRowNums();
		wFields.optWidth(true);
		
		wStepname.selectAll();
	}
	
	private void cancel()
	{
		stepname=null;
		inputMeta.setChanged(changed);
		dispose();
	}
	
	private void getInfo(WordInputMeta inputMeta) {
		
		if (isReceivingInput) {
			inputMeta.setFilenameField(wFilenameField.getText());
			inputMeta.setIncludingFilename(wIncludeFilename.getSelection());
		} else {
			inputMeta.setFilename(wFilename.getText());
		}
		
		inputMeta.setStartRowIndex(wStartRowIndex.getText());
		inputMeta.setTableNr(wTableNr.getText());
		inputMeta.setHeaderPresent(wHeaderPresent.getSelection());
		inputMeta.setRowNumField(wRowNumField.getText());
		inputMeta.setAddResultFile( wAddResult.getSelection() );
		inputMeta.setExtractSpecifiedTable(wExtractSpecifiedTable.getSelection());
		
    	int nrNonEmptyFields = wFields.nrNonEmpty(); 
    	inputMeta.allocate(nrNonEmptyFields);

		for (int i=0;i<nrNonEmptyFields;i++) {
			TableItem item = wFields.getNonEmpty(i);
			inputMeta.getInputFields()[i] = new TextFileInputField();
			
			int colnr=1;
			inputMeta.getInputFields()[i].setName( item.getText(colnr++) );
			inputMeta.getInputFields()[i].setType( ValueMeta.getType( item.getText(colnr++) ) );
			inputMeta.getInputFields()[i].setFormat( item.getText(colnr++) );
			inputMeta.getInputFields()[i].setLength( Const.toInt(item.getText(colnr++), -1) );
			inputMeta.getInputFields()[i].setPrecision( Const.toInt(item.getText(colnr++), -1) );
			inputMeta.getInputFields()[i].setCurrencySymbol( item.getText(colnr++) );
			inputMeta.getInputFields()[i].setDecimalSymbol( item.getText(colnr++) );
			inputMeta.getInputFields()[i].setGroupSymbol( item.getText(colnr++) );
			inputMeta.getInputFields()[i].setTrimType(ValueMeta.getTrimTypeByDesc( item.getText(colnr++) ));
		}
		wFields.removeEmptyRows();
		wFields.setRowNums();
		wFields.optWidth(true);
		
		inputMeta.setChanged();
	}
	
	private void ok()
	{
		if (Const.isEmpty(wStepname.getText())) return;

		getInfo(inputMeta);
		stepname = wStepname.getText();
		dispose();
	}
	
	// Get the data layout
	private void getCSV() 
	{
		InputStream inputStream = null;
		try
		{
			WordInputMeta meta = new WordInputMeta();
			getInfo(meta);
			String filename = transMeta.environmentSubstitute(meta.getFilename());
			FileObject fileObject = KettleVFS.getFileObject(filename);
			if (!(fileObject instanceof LocalFile)) {
				// We can only use NIO on local files at the moment, so that's what we limit ourselves to.
				//
				throw new KettleException(BaseMessages.getString(PKG, "WordInput.Log.OnlyLocalFilesAreSupported"));
			}
			Document doc = new Document(filename);
			Table table = (Table) doc.getChild(NodeType.TABLE, 0, true);
			String[] fieldNames =getOneRow(table,new Integer(meta.getStartRowIndex()).intValue());

			wFields.table.removeAll();
            if (!meta.isHeaderPresent()) {
            	DecimalFormat df = new DecimalFormat("000"); // $NON-NLS-1$
            	for (int i=0;i<fieldNames.length;i++) {
            		fieldNames[i] = "Field_"+df.format(i); // $NON-NLS-1$
            	}
            }

            // Trim the names to make sure...
            //
        	for (int i=0;i<fieldNames.length;i++) {
        		fieldNames[i] = Const.trim(fieldNames[i]);
        	}

            // Update the GUI
            //
            for (int i=0;i<fieldNames.length;i++) {
            	TableItem item = new TableItem(wFields.table, SWT.NONE);
            	item.setText(1, fieldNames[i]);
            	item.setText(2, ValueMeta.getTypeDesc(ValueMetaInterface.TYPE_STRING));
            }
            wFields.removeEmptyRows();
            wFields.setRowNums();
            wFields.optWidth(true);
            
            // Now we can continue reading the rows of data and we can guess the 
            // Sample a few lines to determine the correct type of the fields...
            // 1000 rows as samples
//            String shellText = BaseMessages.getString(PKG, "WordInputDialog.LinesToSample.DialogTitle");
//            String lineText = BaseMessages.getString(PKG, "WordInputDialog.LinesToSample.DialogMessage");

            for(int rowIndex=0;rowIndex<1000;rowIndex++) {
				Object[] r ;
				if(meta.isHeaderPresent())
					r = getOneRow(table, new Integer(meta.getStartRowIndex()).intValue() + meta.getNrHeaderLines()+rowIndex);
				else
					r= getOneRow(table, new Integer(meta.getStartRowIndex()).intValue() +rowIndex);
				if(r == null )//no data now, skip to continue
					continue;
				if (rowIndex==0) {
					List<StringEvaluator> evaluators = new ArrayList<StringEvaluator>();
					int nrfields = meta.getInputFields().length;

					for (int i = 0; i < nrfields && i < r.length; i++) {
						TextFileInputField field = meta.getInputFields()[i];
						StringEvaluator evaluator;
						if (i >= evaluators.size()) {
							evaluator = new StringEvaluator(true);
							evaluators.add(evaluator);
						} else {
							evaluator = evaluators.get(i);
						}

						evaluator.evaluateString(r[i]==null?null:r[i].toString());
						StringEvaluationResult result = evaluator.getAdvicedResult();
						if (result != null) {
							if (result != null) {
								// Take the first option we find, list the others below...
								//
								ValueMetaInterface conversionMeta = result.getConversionMeta();
								field.setType(conversionMeta.getType());
								field.setTrimType(conversionMeta.getTrimType());
								field.setFormat(conversionMeta.getConversionMask());
								field.setDecimalSymbol(conversionMeta.getDecimalSymbol());
								field.setGroupSymbol(conversionMeta.getGroupingSymbol());
								field.setLength(conversionMeta.getLength());
							}
						}
					}
				}
				else {
					int nrfields = meta.getInputFields().length;
					for (int i = 0; i < nrfields && i < r.length; i++) {
						TextFileInputField field = meta.getInputFields()[i];
						if(field.getType()== ValueMetaInterface.TYPE_STRING && r[i]!=null && r[i].toString().length()>field.getLength())
							field.setLength(r[i].toString().length());
					}
				}
			}

			wFields.removeAll();
			getData(meta, false);//write meta to wFields
			wFields.removeEmptyRows();
			wFields.setRowNums();
			wFields.optWidth(true);


		}
		catch(IOException e)
		{
            new ErrorDialog(shell, BaseMessages.getString(PKG, "WordInputDialog.IOError.DialogTitle"), BaseMessages.getString(PKG, "WordInputDialog.IOError.DialogMessage"), e);
		}
        catch(KettleException e)
        {
            new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.Error.Title"), BaseMessages.getString(PKG, "WordInputDialog.ErrorGettingFileDesc.DialogMessage"), e);
        }
        catch(Exception e)
		{
			new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.Error.Title"), BaseMessages.getString(PKG, "WordInputDialog.ErrorGettingFileDesc.DialogMessage"), e);
		}
		finally
		{
			try
			{
				inputStream.close();
			}
			catch(Exception e)
			{					
			}
		}
	}
		private String[] getOneRow(Table table,int rowIndex)
		{
			final ArrayList<String> list = new ArrayList();
			Row row = table.getRows().get(rowIndex);
			if (row==null)
				return  null;
			CellCollection cellc = row.getCells();
			cellc.forEach(new Consumer<Cell>(){
				int outputIndex=0;
				@Override
				public void accept(Cell tt) {
					// TODO Auto-generated method stub
					try {
						String cellText = tt.toString(SaveFormat.TEXT).trim().replaceAll("\\n", "").replaceAll("\\t", "").replaceAll("\\r", "");
						list.add(cellText);
						System.out.println(cellText);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			});
			String[] fieldNames= new String[list.size()];
			for(int i=0;i<list.toArray().length;i++)
			{
				fieldNames[i]=(String)list.toArray()[i];
			}
			return fieldNames;
		}

	// Preview the data
    private void preview()
    {
        // Create the XML input step
        WordInputMeta oneMeta = new WordInputMeta();
        getInfo(oneMeta);
        
        TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(transMeta, oneMeta, wStepname.getText());
        transMeta.getVariable("Internal.Transformation.Filename.Directory");
        previewMeta.getVariable("Internal.Transformation.Filename.Directory");
        
        EnterNumberDialog numberDialog = new EnterNumberDialog(shell, props.getDefaultPreviewSize(), BaseMessages.getString(PKG, "WordInputDialog.PreviewSize.DialogTitle"), BaseMessages.getString(PKG, "WordInputDialog.PreviewSize.DialogMessage"));
        int previewSize = numberDialog.open();
        if (previewSize>0)
        {
            TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog(shell, previewMeta, new String[] { wStepname.getText() }, new int[] { previewSize } );
            progressDialog.open();

            Trans trans = progressDialog.getTrans();
            String loggingText = progressDialog.getLoggingText();

            if (!progressDialog.isCancelled())
            {
                if (trans.getResult()!=null && trans.getResult().getNrErrors()>0)
                {
                	EnterTextDialog etd = new EnterTextDialog(shell, BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),  
                			BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"), loggingText, true );
                	etd.setReadOnly();
                	etd.open();
                }
            }
            
            PreviewRowsDialog prd =new PreviewRowsDialog(shell, transMeta, SWT.NONE, wStepname.getText(), progressDialog.getPreviewRowsMeta(wStepname.getText()), progressDialog.getPreviewRows(wStepname.getText()), loggingText);
            prd.open();
        }
    }

}
