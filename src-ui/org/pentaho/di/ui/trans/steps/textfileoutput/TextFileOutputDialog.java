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

package org.pentaho.di.ui.trans.steps.textfileoutput;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;

import java.nio.charset.Charset;
import java.util.*;
import java.util.List;



public class TextFileOutputDialog extends BaseStepDialog implements StepDialogInterface
{
	private static Class<?> PKG = TextFileOutputMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private CTabFolder   wTabFolder;
	private FormData     fdTabFolder ;
	
	private CTabItem     wFileTab, wContentTab, wRemoveTab, wFieldsTab;

	private FormData     fdFileComp, fdContentComp,fdRemoveComp, fdFieldsComp;

	private Label        wlFilename;
	private Button       wbFilename;
	private TextVar     wFilename;
	private FormData     fdlFilename, fdbFilename, fdFilename;

	private Label        wlFileIsCommand;
	private Button       wFileIsCommand;
	private FormData     fdlFileIsCommand, fdFileIsCommand;

	private Label        wlServletOutput;
	private Button       wServletOutput;
	private FormData     fdlServletOutput, fdServletOutput;

	
	private Label        wlExtension;
	private TextVar         wExtension;
	private FormData     fdlExtension, fdExtension;

	private Label        wlAddStepnr;
	private Button       wAddStepnr;
	private FormData     fdlAddStepnr, fdAddStepnr;

	private Label        wlAddPartnr;
	private Button       wAddPartnr;
	private FormData     fdlAddPartnr, fdAddPartnr;

	private Label        wlAddDate;
	private Button       wAddDate;
	private FormData     fdlAddDate, fdAddDate;

	private Label        wlAddTime;
	private Button       wAddTime;
	private FormData     fdlAddTime, fdAddTime;

	private Button       wbShowFiles;
	private FormData     fdbShowFiles;
	
	/* Additional fields*/
	private Label        wlFileNameInField;
    private Button       wFileNameInField;
    private FormData     fdlFileNameInField, fdFileNameInField;
	
	private Label        wlFileNameField;
	private ComboVar      wFileNameField;
	private FormData     fdlFileNameField, fdFileNameField;
	/*END*/




	private Label        wlAppend;
	private Button       wAppend;
	private FormData     fdlAppend, fdAppend;

	private Label        wlSeparator;
	private Button       wbSeparator;
	private TextVar         wSeparator;
	private FormData     fdlSeparator, fdbSeparator, fdSeparator;

	private Label        wlSeparatorAfterLastColumn;
    private Button       wSeparatorAfterLastColumn;
    private FormData     fdlSeparatorAfterLastColumn, fdSeparatorAfterLastColumn;

	private Label        wlRemoveCRLF;
	private Button       wRemoveCRLF;
	private FormData     fdlRemoveCRLF, fdRemoveCRLF;

	private Label        wlEnclosure;
	private TextVar      wEnclosure;
	private FormData     fdlEnclosure, fdEnclosure;

	private Label        wlEndedLine;
	private Text         wEndedLine;
	private FormData     fdlEndedLine, fdEndedLine;
	
    private Label        wlEnclForced;
    private Button       wEnclForced;
    private FormData     fdlEnclForced, fdEnclForced;
    
    private Label        wlDisableEnclosureFix;
    private Button       wDisableEnclosureFix;
    private FormData     fdlDisableEnclosureFix, fdDisableEnclosureFix;

	private Label        wlHeader;
	private Button       wHeader;
	private FormData     fdlHeader, fdHeader;
	
	private Label        wlFooter;
	private Button       wFooter;
	private FormData     fdlFooter, fdFooter;

	private Label        wlFormat;
	private CCombo       wFormat;
	private FormData     fdlFormat, fdFormat;

	private Label        wlCompression;
	private CCombo       wCompression;
	private FormData     fdlCompression, fdCompression;

    private Label        wlEncoding;
    private CCombo       wEncoding;
    private FormData     fdlEncoding, fdEncoding;

	private Label        wlPad;
	private Button       wPad;
	private FormData     fdlPad, fdPad;

	private Label        wlFastDump;
	private Button       wFastDump;
	private FormData     fdlFastDump, fdFastDump;

	private Label        wlSplitEvery;
	private Text         wSplitEvery;
	private FormData     fdlSplitEvery, fdSplitEvery;


	private Label        wlSplitFileField;
	private ComboVar      wSplitFileField;
	private FormData     fdlSplitFileField, fdSplitFileField;

	private TableView    wFields;
	private FormData     fdFields;

	private TextFileOutputMeta input;
	
    private Button       wMinWidth;
    private Listener     lsMinWidth;
    private boolean      gotEncodings = false; 
    
	private Label        wlAddToResult;
	private Button       wAddToResult;
	private FormData     fdlAddToResult, fdAddToResult;
	
	private Label        wlDoNotOpenNewFileInit;
	private Button       wDoNotOpenNewFileInit;
	private FormData     fdlDoNotOpenNewFileInit, fdDoNotOpenNewFileInit;
	
  	private Label        wlDateTimeFormat;
	private CCombo       wDateTimeFormat;
	private FormData     fdlDateTimeFormat, fdDateTimeFormat; 
	
	private Label        wlSpecifyFormat;
	private Button       wSpecifyFormat;
	private FormData     fdlSpecifyFormat, fdSpecifyFormat;
	
	private Label        wlCreateParentFolder;
	private Button       wCreateParentFolder;
	private FormData     fdlCreateParentFolder, fdCreateParentFolder;


	private Label        wlRemove;
	private TableView    wRemove;
	private FormData     fdlRemove, fdRemove;

	private Button wGetRemove;
	private FormData fdGetRemove;

	private List<ColumnInfo> fieldColumns = new ArrayList<ColumnInfo>();

	
	private ColumnInfo[] colinf;

    private Map<String, Integer> inputFields;
    
    private boolean gotPreviousFields=false;
	private boolean gotPreviousFields2=false;

    
	public TextFileOutputDialog(Shell parent, Object in, TransMeta transMeta, String sname)
	{
		super(parent, (BaseStepMeta)in, transMeta, sname);
		input=(TextFileOutputMeta)in;
        inputFields =new HashMap<String, Integer>();
	}

	public String open()
	{
        Shell parent = getParent();
		Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
 		props.setLook(shell);
        setShellImage(shell, input);

		ModifyListener lsMod = new ModifyListener() 
		{
			public void modifyText(ModifyEvent e) 
			{
				input.setChanged();
			}
		};
		changed = input.hasChanged();
		
		FormLayout formLayout = new FormLayout ();
		formLayout.marginWidth  = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.DialogTitle"));
		
		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname=new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "System.Label.StepName"));
 		props.setLook(wlStepname);
		fdlStepname=new FormData();
		fdlStepname.left  = new FormAttachment(0, 0);
		fdlStepname.top   = new FormAttachment(0, margin);
		fdlStepname.right = new FormAttachment(middle, -margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname=new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
 		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname=new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top  = new FormAttachment(0, margin);
		fdStepname.right= new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		wTabFolder = new CTabFolder(shell, SWT.BORDER);
 		props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);
 		wTabFolder.setSimple(false);
 				
		//////////////////////////
		// START OF FILE TAB///
		///
		wFileTab=new CTabItem(wTabFolder, SWT.NONE);
		wFileTab.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FileTab.TabTitle"));
		
		Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
 		props.setLook(wFileComp);

		FormLayout fileLayout = new FormLayout();
		fileLayout.marginWidth  = 3;
		fileLayout.marginHeight = 3;
		wFileComp.setLayout(fileLayout);

		// Filename line
		wlFilename=new Label(wFileComp, SWT.RIGHT);
		wlFilename.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Filename.Label"));
 		props.setLook(wlFilename);
		fdlFilename=new FormData();
		fdlFilename.left = new FormAttachment(0, 0);
		fdlFilename.top  = new FormAttachment(0, margin);
		fdlFilename.right= new FormAttachment(middle, -margin);
		wlFilename.setLayoutData(fdlFilename);

		wbFilename=new Button(wFileComp, SWT.PUSH| SWT.CENTER);
 		props.setLook(wbFilename);
		wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
		fdbFilename=new FormData();
		fdbFilename.right= new FormAttachment(100, 0);
		fdbFilename.top  = new FormAttachment(0, 0);
		wbFilename.setLayoutData(fdbFilename);

		wFilename=new TextVar(transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wFilename);
		wFilename.addModifyListener(lsMod);
		fdFilename=new FormData();
		fdFilename.left = new FormAttachment(middle, 0);
		fdFilename.top  = new FormAttachment(0, margin);
		fdFilename.right= new FormAttachment(wbFilename, -margin);
		wFilename.setLayoutData(fdFilename);
		

		// Run this as a command instead?
		wlFileIsCommand=new Label(wFileComp, SWT.RIGHT);
		wlFileIsCommand.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FileIsCommand.Label"));
 		props.setLook(wlFileIsCommand);
		fdlFileIsCommand=new FormData();
		fdlFileIsCommand.left = new FormAttachment(0, 0);
		fdlFileIsCommand.top  = new FormAttachment(wFilename, margin);
		fdlFileIsCommand.right= new FormAttachment(middle, -margin);
		wlFileIsCommand.setLayoutData(fdlFileIsCommand);
		wFileIsCommand=new Button(wFileComp, SWT.CHECK);
 		props.setLook(wFileIsCommand);
		fdFileIsCommand=new FormData();
		fdFileIsCommand.left = new FormAttachment(middle, 0);
		fdFileIsCommand.top  = new FormAttachment(wFilename, margin);
		fdFileIsCommand.right= new FormAttachment(100, 0);
		wFileIsCommand.setLayoutData(fdFileIsCommand);
		wFileIsCommand.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
					enableParentFolder();
				}
			}
		);

    // Output to servlet (browser, ws)
		//
    wlServletOutput=new Label(wFileComp, SWT.RIGHT);
    wlServletOutput.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.ServletOutput.Label"));
    props.setLook(wlServletOutput);
    fdlServletOutput=new FormData();
    fdlServletOutput.left = new FormAttachment(0, 0);
    fdlServletOutput.top  = new FormAttachment(wFileIsCommand, margin);
    fdlServletOutput.right= new FormAttachment(middle, -margin);
    wlServletOutput.setLayoutData(fdlServletOutput);
    wServletOutput=new Button(wFileComp, SWT.CHECK);
    wServletOutput.setToolTipText(BaseMessages.getString(PKG, "TextFileOutputDialog.ServletOutput.Tooltip"));
    props.setLook(wServletOutput);
    fdServletOutput=new FormData();
    fdServletOutput.left = new FormAttachment(middle, 0);
    fdServletOutput.top  = new FormAttachment(wFileIsCommand, margin);
    fdServletOutput.right= new FormAttachment(100, 0);
    wServletOutput.setLayoutData(fdServletOutput);
    wServletOutput.addSelectionListener(new SelectionAdapter() 
      {
        public void widgetSelected(SelectionEvent e) 
        {
          input.setChanged();
          setFlagsServletOption();
        }
      }
    );

		

		// Create Parent Folder
		wlCreateParentFolder=new Label(wFileComp, SWT.RIGHT);
		wlCreateParentFolder.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.CreateParentFolder.Label"));
 		props.setLook(wlCreateParentFolder);
		fdlCreateParentFolder=new FormData();
		fdlCreateParentFolder.left = new FormAttachment(0, 0);
		fdlCreateParentFolder.top  = new FormAttachment(wServletOutput, margin);
		fdlCreateParentFolder.right= new FormAttachment(middle, -margin);
		wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
		wCreateParentFolder=new Button(wFileComp, SWT.CHECK );
		wCreateParentFolder.setToolTipText(BaseMessages.getString(PKG, "TextFileOutputDialog.CreateParentFolder.Tooltip"));
 		props.setLook(wCreateParentFolder);
		fdCreateParentFolder=new FormData();
		fdCreateParentFolder.left = new FormAttachment(middle, 0);
		fdCreateParentFolder.top  = new FormAttachment(wServletOutput, margin);
		fdCreateParentFolder.right= new FormAttachment(100, 0);
		wCreateParentFolder.setLayoutData(fdCreateParentFolder);
		wCreateParentFolder.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);
		



		// Open new File at Init
		wlDoNotOpenNewFileInit=new Label(wFileComp, SWT.RIGHT);
		wlDoNotOpenNewFileInit.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.DoNotOpenNewFileInit.Label"));
 		props.setLook(wlDoNotOpenNewFileInit);
		fdlDoNotOpenNewFileInit=new FormData();
		fdlDoNotOpenNewFileInit.left = new FormAttachment(0, 0);
		fdlDoNotOpenNewFileInit.top  = new FormAttachment(wCreateParentFolder, margin);
		fdlDoNotOpenNewFileInit.right= new FormAttachment(middle, -margin);
		wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
		wDoNotOpenNewFileInit=new Button(wFileComp, SWT.CHECK );
		wDoNotOpenNewFileInit.setToolTipText(BaseMessages.getString(PKG, "TextFileOutputDialog.DoNotOpenNewFileInit.Tooltip"));
 		props.setLook(wDoNotOpenNewFileInit);
		fdDoNotOpenNewFileInit=new FormData();
		fdDoNotOpenNewFileInit.left = new FormAttachment(middle, 0);
		fdDoNotOpenNewFileInit.top  = new FormAttachment(wCreateParentFolder, margin);
		fdDoNotOpenNewFileInit.right= new FormAttachment(100, 0);
		wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
		wDoNotOpenNewFileInit.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);
		
		/*next Lines*/
		// FileNameInField line
        wlFileNameInField=new Label(wFileComp, SWT.RIGHT);
        wlFileNameInField.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FileNameInField.Label"));
        props.setLook(wlFileNameInField);
        fdlFileNameInField=new FormData();
        fdlFileNameInField.left = new FormAttachment(0, 0);
        fdlFileNameInField.top  = new FormAttachment(wDoNotOpenNewFileInit, margin);
        fdlFileNameInField.right= new FormAttachment(middle, -margin);
        wlFileNameInField.setLayoutData(fdlFileNameInField);
        wFileNameInField=new Button(wFileComp, SWT.CHECK );
        props.setLook(wFileNameInField);
        fdFileNameInField=new FormData();
        fdFileNameInField.left = new FormAttachment(middle, 0);
        fdFileNameInField.top  = new FormAttachment(wDoNotOpenNewFileInit, margin);
        fdFileNameInField.right= new FormAttachment(100, 0);
        wFileNameInField.setLayoutData(fdFileNameInField);
        wFileNameInField.addSelectionListener(new SelectionAdapter() 
            {
                public void widgetSelected(SelectionEvent e) 
                {
                	input.setChanged();
                	activeFileNameField();        
                }
            }
        );

		// FileNameField Line
		wlFileNameField=new Label(wFileComp, SWT.RIGHT);
		wlFileNameField.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FileNameField.Label")); //$NON-NLS-1$
 		props.setLook(wlFileNameField);
		fdlFileNameField=new FormData();
		fdlFileNameField.left = new FormAttachment(0, 0);
		fdlFileNameField.right= new FormAttachment(middle, -margin);
		fdlFileNameField.top  = new FormAttachment(wFileNameInField, margin);
		wlFileNameField.setLayoutData(fdlFileNameField);
		
    	wFileNameField=new ComboVar(transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wFileNameField);
		wFileNameField.addModifyListener(lsMod);
		fdFileNameField=new FormData();
		fdFileNameField.left = new FormAttachment(middle, 0);
		fdFileNameField.top  = new FormAttachment(wFileNameInField, margin);
		fdFileNameField.right= new FormAttachment(100, 0);
		wFileNameField.setLayoutData(fdFileNameField);
		wFileNameField.setEnabled(false);
		wFileNameField.addFocusListener(new FocusListener()
        {
            public void focusLost(org.eclipse.swt.events.FocusEvent e)
            {
            }
        
            public void focusGained(org.eclipse.swt.events.FocusEvent e)
            {
                Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                shell.setCursor(busy);
                getFields();
                shell.setCursor(null);
                busy.dispose();
            }
        }
    );  
		/*End*/

		// Extension line
		wlExtension=new Label(wFileComp, SWT.RIGHT);
		wlExtension.setText(BaseMessages.getString(PKG, "System.Label.Extension"));
 		props.setLook(wlExtension);
		fdlExtension=new FormData();
		fdlExtension.left = new FormAttachment(0, 0);
		fdlExtension.top  = new FormAttachment(wFileNameField, margin);
		fdlExtension.right= new FormAttachment(middle, -margin);
		wlExtension.setLayoutData(fdlExtension);
		wExtension=new TextVar(transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wExtension.setText("");
 		props.setLook(wExtension);
		wExtension.addModifyListener(lsMod);
		fdExtension=new FormData();
		fdExtension.left = new FormAttachment(middle, 0);
		fdExtension.top  = new FormAttachment(wFileNameField, margin);
		fdExtension.right= new FormAttachment(100, 0);
		wExtension.setLayoutData(fdExtension);

		// Create multi-part file?
		wlAddStepnr=new Label(wFileComp, SWT.RIGHT);
		wlAddStepnr.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddStepnr.Label"));
 		props.setLook(wlAddStepnr);
		fdlAddStepnr=new FormData();
		fdlAddStepnr.left = new FormAttachment(0, 0);
		fdlAddStepnr.top  = new FormAttachment(wExtension, margin);
		fdlAddStepnr.right= new FormAttachment(middle, -margin);
		wlAddStepnr.setLayoutData(fdlAddStepnr);
		wAddStepnr=new Button(wFileComp, SWT.CHECK);
 		props.setLook(wAddStepnr);
		fdAddStepnr=new FormData();
		fdAddStepnr.left = new FormAttachment(middle, 0);
		fdAddStepnr.top  = new FormAttachment(wExtension, margin);
		fdAddStepnr.right= new FormAttachment(100, 0);
		wAddStepnr.setLayoutData(fdAddStepnr);
		wAddStepnr.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);

		// Create multi-part file?
		wlAddPartnr=new Label(wFileComp, SWT.RIGHT);
		wlAddPartnr.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddPartnr.Label"));
 		props.setLook(wlAddPartnr);
		fdlAddPartnr=new FormData();
		fdlAddPartnr.left = new FormAttachment(0, 0);
		fdlAddPartnr.top  = new FormAttachment(wAddStepnr, margin);
		fdlAddPartnr.right= new FormAttachment(middle, -margin);
		wlAddPartnr.setLayoutData(fdlAddPartnr);
		wAddPartnr=new Button(wFileComp, SWT.CHECK);
 		props.setLook(wAddPartnr);
		fdAddPartnr=new FormData();
		fdAddPartnr.left = new FormAttachment(middle, 0);
		fdAddPartnr.top  = new FormAttachment(wAddStepnr, margin);
		fdAddPartnr.right= new FormAttachment(100, 0);
		wAddPartnr.setLayoutData(fdAddPartnr);
		wAddPartnr.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);

		// Create multi-part file?
		wlAddDate=new Label(wFileComp, SWT.RIGHT);
		wlAddDate.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddDate.Label"));
 		props.setLook(wlAddDate);
		fdlAddDate=new FormData();
		fdlAddDate.left = new FormAttachment(0, 0);
		fdlAddDate.top  = new FormAttachment(wAddPartnr, margin);
		fdlAddDate.right= new FormAttachment(middle, -margin);
		wlAddDate.setLayoutData(fdlAddDate);
		wAddDate=new Button(wFileComp, SWT.CHECK);
 		props.setLook(wAddDate);
		fdAddDate=new FormData();
		fdAddDate.left = new FormAttachment(middle, 0);
		fdAddDate.top  = new FormAttachment(wAddPartnr, margin);
		fdAddDate.right= new FormAttachment(100, 0);
		wAddDate.setLayoutData(fdAddDate);
		wAddDate.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
					// System.out.println("wAddDate.getSelection()="+wAddDate.getSelection());
				}
			}
		);
		// Create multi-part file?
		wlAddTime=new Label(wFileComp, SWT.RIGHT);
		wlAddTime.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddTime.Label"));
 		props.setLook(wlAddTime);
		fdlAddTime=new FormData();
		fdlAddTime.left = new FormAttachment(0, 0);
		fdlAddTime.top  = new FormAttachment(wAddDate, margin);
		fdlAddTime.right= new FormAttachment(middle, -margin);
		wlAddTime.setLayoutData(fdlAddTime);
		wAddTime=new Button(wFileComp, SWT.CHECK);
 		props.setLook(wAddTime);
		fdAddTime=new FormData();
		fdAddTime.left = new FormAttachment(middle, 0);
		fdAddTime.top  = new FormAttachment(wAddDate, margin);
		fdAddTime.right= new FormAttachment(100, 0);
		wAddTime.setLayoutData(fdAddTime);
		wAddTime.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);
		
		// Specify date time format?
		wlSpecifyFormat=new Label(wFileComp, SWT.RIGHT);
		wlSpecifyFormat.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.SpecifyFormat.Label"));
		props.setLook(wlSpecifyFormat);
		fdlSpecifyFormat=new FormData();
		fdlSpecifyFormat.left = new FormAttachment(0, 0);
		fdlSpecifyFormat.top  = new FormAttachment(wAddTime, margin);
		fdlSpecifyFormat.right= new FormAttachment(middle, -margin);
		wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
		wSpecifyFormat=new Button(wFileComp, SWT.CHECK);
		props.setLook(wSpecifyFormat);
		wSpecifyFormat.setToolTipText(BaseMessages.getString(PKG, "TextFileOutputDialog.SpecifyFormat.Tooltip"));
	    fdSpecifyFormat=new FormData();
		fdSpecifyFormat.left = new FormAttachment(middle, 0);
		fdSpecifyFormat.top  = new FormAttachment(wAddTime, margin);
		fdSpecifyFormat.right= new FormAttachment(100, 0);
		wSpecifyFormat.setLayoutData(fdSpecifyFormat);
		wSpecifyFormat.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
					setDateTimeFormat();
				}
			}
		);
		
 		// DateTimeFormat
		wlDateTimeFormat=new Label(wFileComp, SWT.RIGHT);
        wlDateTimeFormat.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.DateTimeFormat.Label"));
        props.setLook(wlDateTimeFormat);
        fdlDateTimeFormat=new FormData();
        fdlDateTimeFormat.left = new FormAttachment(0, 0);
        fdlDateTimeFormat.top  = new FormAttachment(wSpecifyFormat, margin);
        fdlDateTimeFormat.right= new FormAttachment(middle, -margin);
        wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
        wDateTimeFormat=new CCombo(wFileComp, SWT.BORDER | SWT.READ_ONLY);
        wDateTimeFormat.setEditable(true);
        props.setLook(wDateTimeFormat);
        wDateTimeFormat.addModifyListener(lsMod);
        fdDateTimeFormat=new FormData();
        fdDateTimeFormat.left = new FormAttachment(middle, 0);
        fdDateTimeFormat.top  = new FormAttachment(wSpecifyFormat, margin);
        fdDateTimeFormat.right= new FormAttachment(100, 0);
        wDateTimeFormat.setLayoutData(fdDateTimeFormat);
		String dats[] = Const.getDateFormats();
        for (int x=0;x<dats.length;x++) wDateTimeFormat.add(dats[x]);
        


		wbShowFiles=new Button(wFileComp, SWT.PUSH| SWT.CENTER);
 		props.setLook(wbShowFiles);
		wbShowFiles.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.ShowFiles.Button"));
		fdbShowFiles=new FormData();
		fdbShowFiles.left = new FormAttachment(middle, 0);
		fdbShowFiles.top  = new FormAttachment(wDateTimeFormat, margin*2);
		wbShowFiles.setLayoutData(fdbShowFiles);
		wbShowFiles.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					TextFileOutputMeta tfoi = new TextFileOutputMeta();
					getInfo(tfoi);
					String files[] = tfoi.getFiles(transMeta);
					if (files!=null && files.length>0)
					{
						EnterSelectionDialog esd = new EnterSelectionDialog(shell, files, BaseMessages.getString(PKG, "TextFileOutputDialog.SelectOutputFiles.DialogTitle"), BaseMessages.getString(PKG, "TextFileOutputDialog.SelectOutputFiles.DialogMessage"));
						esd.setViewOnly();
						esd.open();
					}
					else
					{
						MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR );
						mb.setMessage(BaseMessages.getString(PKG, "TextFileOutputDialog.NoFilesFound.DialogMessage"));
						mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
						mb.open(); 
					}
				}
			}
		);


		// Add File to the result files name
		wlAddToResult=new Label(wFileComp, SWT.RIGHT);
		wlAddToResult.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddFileToResult.Label"));
		props.setLook(wlAddToResult);
		fdlAddToResult=new FormData();
		fdlAddToResult.left  = new FormAttachment(0, 0);
		fdlAddToResult.top   = new FormAttachment(wbShowFiles, 2*margin);
		fdlAddToResult.right = new FormAttachment(middle, -margin);
		wlAddToResult.setLayoutData(fdlAddToResult);
		wAddToResult=new Button(wFileComp, SWT.CHECK);
		wAddToResult.setToolTipText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddFileToResult.Tooltip"));
 		props.setLook(wAddToResult);
		fdAddToResult=new FormData();
		fdAddToResult.left  = new FormAttachment(middle, 0);
		fdAddToResult.top   = new FormAttachment(wbShowFiles, 2*margin);
		fdAddToResult.right = new FormAttachment(100, 0);
		wAddToResult.setLayoutData(fdAddToResult);
		SelectionAdapter lsSelR = new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent arg0)
            {
                input.setChanged();
            }
        };
		wAddToResult.addSelectionListener(lsSelR);

		
		fdFileComp=new FormData();
		fdFileComp.left  = new FormAttachment(0, 0);
		fdFileComp.top   = new FormAttachment(0, 0);
		fdFileComp.right = new FormAttachment(100, 0);
		fdFileComp.bottom= new FormAttachment(100, 0);
		wFileComp.setLayoutData(fdFileComp);
	
		wFileComp.layout();
		wFileTab.setControl(wFileComp);

		/////////////////////////////////////////////////////////////
		/// END OF FILE TAB
		/////////////////////////////////////////////////////////////

		//////////////////////////
		// START OF CONTENT TAB///
		///
		wContentTab=new CTabItem(wTabFolder, SWT.NONE);
		wContentTab.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.ContentTab.TabTitle"));

		FormLayout contentLayout = new FormLayout ();
		contentLayout.marginWidth  = 3;
		contentLayout.marginHeight = 3;
		
		Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
 		props.setLook(wContentComp);
		wContentComp.setLayout(contentLayout);


		// Append to end of file?
		wlAppend=new Label(wContentComp, SWT.RIGHT);
		wlAppend.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Append.Label"));
 		props.setLook(wlAppend);
		fdlAppend=new FormData();
		fdlAppend.left = new FormAttachment(0, 0);
		fdlAppend.top  = new FormAttachment(0, 0);
		fdlAppend.right= new FormAttachment(middle, -margin);
		wlAppend.setLayoutData(fdlAppend);
		wAppend=new Button(wContentComp, SWT.CHECK);
 		props.setLook(wAppend);
		fdAppend=new FormData();
		fdAppend.left = new FormAttachment(middle, 0);
		fdAppend.top  = new FormAttachment(0, 0);
		fdAppend.right= new FormAttachment(100, 0);
		wAppend.setLayoutData(fdAppend);
		wAppend.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);
		
		wlSeparator=new Label(wContentComp, SWT.RIGHT);
		wlSeparator.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Separator.Label"));
 		props.setLook(wlSeparator);
		fdlSeparator=new FormData();
		fdlSeparator.left = new FormAttachment(0, 0);
		fdlSeparator.top  = new FormAttachment(wAppend, margin);
		fdlSeparator.right= new FormAttachment(middle, -margin);
		wlSeparator.setLayoutData(fdlSeparator);

		wbSeparator=new Button(wContentComp, SWT.PUSH| SWT.CENTER);
 		props.setLook(wbSeparator);
		wbSeparator.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Separator.Button"));
		fdbSeparator=new FormData();
		fdbSeparator.right= new FormAttachment(100, 0);
		fdbSeparator.top  = new FormAttachment(wAppend, 0);
		wbSeparator.setLayoutData(fdbSeparator);
		wbSeparator.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent se) 
				{
					//wSeparator.insert("\t");
					wSeparator.getTextWidget().insert("\t");
				}
			}
		);

		wSeparator=new TextVar(transMeta,wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wSeparator);
		wSeparator.addModifyListener(lsMod);
		fdSeparator=new FormData();
		fdSeparator.left = new FormAttachment(middle, 0);
		fdSeparator.top  = new FormAttachment(wAppend, margin);
		fdSeparator.right= new FormAttachment(wbSeparator, -margin);
		wSeparator.setLayoutData(fdSeparator);

		//sepatator after last column line... jason 2016
		wlSeparatorAfterLastColumn=new Label(wContentComp, SWT.RIGHT);
		wlSeparatorAfterLastColumn.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.SepatatorLastColumn.Label"));
        props.setLook(wlSeparatorAfterLastColumn);
        fdlSeparatorAfterLastColumn=new FormData();
        fdlSeparatorAfterLastColumn.left = new FormAttachment(0, 0);
        fdlSeparatorAfterLastColumn.top  = new FormAttachment(wSeparator, margin);
        fdlSeparatorAfterLastColumn.right= new FormAttachment(middle, -margin);
        wlSeparatorAfterLastColumn.setLayoutData(fdlSeparatorAfterLastColumn);
        wSeparatorAfterLastColumn=new Button(wContentComp, SWT.CHECK );
        props.setLook(wSeparatorAfterLastColumn);
        fdSeparatorAfterLastColumn=new FormData();
        fdSeparatorAfterLastColumn.left = new FormAttachment(middle, 0);
        fdSeparatorAfterLastColumn.top  = new FormAttachment(wSeparator, margin);
        fdSeparatorAfterLastColumn.right= new FormAttachment(100, 0);
        wSeparatorAfterLastColumn.setLayoutData(fdSeparatorAfterLastColumn);
        wSeparatorAfterLastColumn.addSelectionListener(new SelectionAdapter() 
            {
                public void widgetSelected(SelectionEvent e) 
                {
                    input.setChanged();
                }
            }
        );

		//remove CRLF jason 2016
		wlRemoveCRLF=new Label(wContentComp, SWT.RIGHT);
		wlRemoveCRLF.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.RemoveCRLF.Label"));
		props.setLook(wlRemoveCRLF);
		fdlRemoveCRLF=new FormData();
		fdlRemoveCRLF.left = new FormAttachment(0, 0);
		fdlRemoveCRLF.top  = new FormAttachment(wSeparatorAfterLastColumn, margin);
		fdlRemoveCRLF.right= new FormAttachment(middle, -margin);
		wlRemoveCRLF.setLayoutData(fdlRemoveCRLF);
		wRemoveCRLF=new Button(wContentComp, SWT.CHECK );
		props.setLook(wRemoveCRLF);
		fdRemoveCRLF=new FormData();
		fdRemoveCRLF.left = new FormAttachment(middle, 0);
		fdRemoveCRLF.top  = new FormAttachment(wSeparatorAfterLastColumn, margin);
		fdRemoveCRLF.right= new FormAttachment(100, 0);
		wRemoveCRLF.setLayoutData(fdRemoveCRLF);
		wRemoveCRLF.addSelectionListener(new SelectionAdapter()
													   {
														   public void widgetSelected(SelectionEvent e)
														   {
															   input.setChanged();
														   }
													   }
		);


		// Enclosure line...
		wlEnclosure=new Label(wContentComp, SWT.RIGHT);
		wlEnclosure.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Enclosure.Label"));
 		props.setLook(wlEnclosure);
		fdlEnclosure=new FormData();
		fdlEnclosure.left = new FormAttachment(0, 0);
		fdlEnclosure.top  = new FormAttachment(wRemoveCRLF, margin);
		fdlEnclosure.right= new FormAttachment(middle, -margin);
		wlEnclosure.setLayoutData(fdlEnclosure);
		wEnclosure=new TextVar(transMeta, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wEnclosure);
		wEnclosure.addModifyListener(lsMod);
		fdEnclosure=new FormData();
		fdEnclosure.left = new FormAttachment(middle, 0);
		fdEnclosure.top  = new FormAttachment(wRemoveCRLF, margin);
		fdEnclosure.right= new FormAttachment(100, 0);
		wEnclosure.setLayoutData(fdEnclosure);

        wlEnclForced=new Label(wContentComp, SWT.RIGHT);
        wlEnclForced.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.EnclForced.Label"));
        props.setLook(wlEnclForced);
        fdlEnclForced=new FormData();
        fdlEnclForced.left = new FormAttachment(0, 0);
        fdlEnclForced.top  = new FormAttachment(wEnclosure, margin);
        fdlEnclForced.right= new FormAttachment(middle, -margin);
        wlEnclForced.setLayoutData(fdlEnclForced);
        wEnclForced=new Button(wContentComp, SWT.CHECK );
        props.setLook(wEnclForced);
        fdEnclForced=new FormData();
        fdEnclForced.left = new FormAttachment(middle, 0);
        fdEnclForced.top  = new FormAttachment(wEnclosure, margin);
        fdEnclForced.right= new FormAttachment(100, 0);
        wEnclForced.setLayoutData(fdEnclForced);
        wEnclForced.addSelectionListener(new SelectionAdapter() 
            {
                public void widgetSelected(SelectionEvent e) 
                {
                    input.setChanged();
                }
            }
        );
        
        wlDisableEnclosureFix=new Label(wContentComp, SWT.RIGHT);
        wlDisableEnclosureFix.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.DisableEnclosureFix.Label"));
        props.setLook(wlDisableEnclosureFix);
        fdlDisableEnclosureFix=new FormData();
        fdlDisableEnclosureFix.left = new FormAttachment(0, 0);
        fdlDisableEnclosureFix.top  = new FormAttachment(wEnclForced, margin);
        fdlDisableEnclosureFix.right= new FormAttachment(middle, -margin);
        wlDisableEnclosureFix.setLayoutData(fdlDisableEnclosureFix);
        wDisableEnclosureFix=new Button(wContentComp, SWT.CHECK );
        props.setLook(wDisableEnclosureFix);
        fdDisableEnclosureFix=new FormData();
        fdDisableEnclosureFix.left = new FormAttachment(middle, 0);
        fdDisableEnclosureFix.top  = new FormAttachment(wEnclForced, margin);
        fdDisableEnclosureFix.right= new FormAttachment(100, 0);
        wDisableEnclosureFix.setLayoutData(fdDisableEnclosureFix);
        wDisableEnclosureFix.addSelectionListener(new SelectionAdapter() 
            {
                public void widgetSelected(SelectionEvent e) 
                {
                    input.setChanged();
                }
            }
        );

		wlHeader=new Label(wContentComp, SWT.RIGHT);
		wlHeader.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Header.Label"));
 		props.setLook(wlHeader);
		fdlHeader=new FormData();
		fdlHeader.left = new FormAttachment(0, 0);
		fdlHeader.top  = new FormAttachment(wDisableEnclosureFix, margin);
		fdlHeader.right= new FormAttachment(middle, -margin);
		wlHeader.setLayoutData(fdlHeader);
		wHeader=new Button(wContentComp, SWT.CHECK );
 		props.setLook(wHeader);
		fdHeader=new FormData();
		fdHeader.left = new FormAttachment(middle, 0);
		fdHeader.top  = new FormAttachment(wDisableEnclosureFix, margin);
		fdHeader.right= new FormAttachment(100, 0);
		wHeader.setLayoutData(fdHeader);
		wHeader.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);

		wlFooter=new Label(wContentComp, SWT.RIGHT);
		wlFooter.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Footer.Label"));
 		props.setLook(wlFooter);
		fdlFooter=new FormData();
		fdlFooter.left = new FormAttachment(0, 0);
		fdlFooter.top  = new FormAttachment(wHeader, margin);
		fdlFooter.right= new FormAttachment(middle, -margin);
		wlFooter.setLayoutData(fdlFooter);
		wFooter=new Button(wContentComp, SWT.CHECK );
 		props.setLook(wFooter);
		fdFooter=new FormData();
		fdFooter.left = new FormAttachment(middle, 0);
		fdFooter.top  = new FormAttachment(wHeader, margin);
		fdFooter.right= new FormAttachment(100, 0);
		wFooter.setLayoutData(fdFooter);
		wFooter.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);

		wlFormat=new Label(wContentComp, SWT.RIGHT);
		wlFormat.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Format.Label"));
 		props.setLook(wlFormat);
		fdlFormat=new FormData();
		fdlFormat.left = new FormAttachment(0, 0);
		fdlFormat.top  = new FormAttachment(wFooter, margin);
		fdlFormat.right= new FormAttachment(middle, -margin);
		wlFormat.setLayoutData(fdlFormat);
		wFormat=new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
		wFormat.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Format.Label"));
 		props.setLook(wFormat);

		for (int i=0;i<TextFileOutputMeta.formatMapperLineTerminator.length;i++) {
			// add e.g. TextFileOutputDialog.Format.DOS, .UNIX, .CR, .None
			wFormat.add(BaseMessages.getString(PKG, "TextFileOutputDialog.Format."+TextFileOutputMeta.formatMapperLineTerminator[i])); //$NON-NLS-1$
		}
		wFormat.select(0);
		wFormat.addModifyListener(lsMod);
		fdFormat=new FormData();
		fdFormat.left = new FormAttachment(middle, 0);
		fdFormat.top  = new FormAttachment(wFooter, margin);
		fdFormat.right= new FormAttachment(100, 0);
		wFormat.setLayoutData(fdFormat);

		wlCompression=new Label(wContentComp, SWT.RIGHT);
		wlCompression.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Compression.Label"));
 		props.setLook(wlCompression);
		fdlCompression=new FormData();
		fdlCompression.left = new FormAttachment(0, 0);
		fdlCompression.top  = new FormAttachment(wFormat, margin);
		fdlCompression.right= new FormAttachment(middle, -margin);
		wlCompression.setLayoutData(fdlCompression);
		wCompression=new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
		wCompression.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Compression.Label"));
 		props.setLook(wCompression);

		wCompression.setItems(TextFileOutputMeta.fileCompressionTypeCodes);
		wCompression.addModifyListener(lsMod);
		fdCompression=new FormData();
		fdCompression.left = new FormAttachment(middle, 0);
		fdCompression.top  = new FormAttachment(wFormat, margin);
		fdCompression.right= new FormAttachment(100, 0);
		wCompression.setLayoutData(fdCompression);

        wlEncoding=new Label(wContentComp, SWT.RIGHT);
        wlEncoding.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Encoding.Label"));
        props.setLook(wlEncoding);
        fdlEncoding=new FormData();
        fdlEncoding.left = new FormAttachment(0, 0);
        fdlEncoding.top  = new FormAttachment(wCompression, margin);
        fdlEncoding.right= new FormAttachment(middle, -margin);
        wlEncoding.setLayoutData(fdlEncoding);
        wEncoding=new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
        wEncoding.setEditable(true);
        props.setLook(wEncoding);
        wEncoding.addModifyListener(lsMod);
        fdEncoding=new FormData();
        fdEncoding.left = new FormAttachment(middle, 0);
        fdEncoding.top  = new FormAttachment(wCompression, margin);
        fdEncoding.right= new FormAttachment(100, 0);
        wEncoding.setLayoutData(fdEncoding);
        wEncoding.addFocusListener(new FocusListener()
            {
                public void focusLost(org.eclipse.swt.events.FocusEvent e)
                {
                }
            
                public void focusGained(org.eclipse.swt.events.FocusEvent e)
                {
                    Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
                    shell.setCursor(busy);
                    setEncodings();
                    shell.setCursor(null);
                    busy.dispose();
                }
            }
        );

        
		wlPad=new Label(wContentComp, SWT.RIGHT);
		wlPad.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Pad.Label"));
 		props.setLook(wlPad);
		fdlPad=new FormData();
		fdlPad.left = new FormAttachment(0, 0);
		fdlPad.top  = new FormAttachment(wEncoding, margin);
		fdlPad.right= new FormAttachment(middle, -margin);
		wlPad.setLayoutData(fdlPad);
		wPad=new Button(wContentComp, SWT.CHECK );
 		props.setLook(wPad);
		fdPad=new FormData();
		fdPad.left = new FormAttachment(middle, 0);
		fdPad.top  = new FormAttachment(wEncoding, margin);
		fdPad.right= new FormAttachment(100, 0);
		wPad.setLayoutData(fdPad);
		wPad.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);


		wlFastDump=new Label(wContentComp, SWT.RIGHT);
		wlFastDump.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FastDump.Label"));
 		props.setLook(wlFastDump);
		fdlFastDump=new FormData();
		fdlFastDump.left = new FormAttachment(0, 0);
		fdlFastDump.top  = new FormAttachment(wPad, margin);
		fdlFastDump.right= new FormAttachment(middle, -margin);
		wlFastDump.setLayoutData(fdlFastDump);
		wFastDump=new Button(wContentComp, SWT.CHECK );
 		props.setLook(wFastDump);
		fdFastDump=new FormData();
		fdFastDump.left = new FormAttachment(middle, 0);
		fdFastDump.top  = new FormAttachment(wPad, margin);
		fdFastDump.right= new FormAttachment(100, 0);
		wFastDump.setLayoutData(fdFastDump);
		wFastDump.addSelectionListener(new SelectionAdapter() 
			{
				public void widgetSelected(SelectionEvent e) 
				{
					input.setChanged();
				}
			}
		);


		wlSplitEvery=new Label(wContentComp, SWT.RIGHT);
		wlSplitEvery.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.SplitEvery.Label"));
 		props.setLook(wlSplitEvery);
		fdlSplitEvery=new FormData();
		fdlSplitEvery.left = new FormAttachment(0, 0);
		fdlSplitEvery.top  = new FormAttachment(wFastDump, margin);
		fdlSplitEvery.right= new FormAttachment(middle, -margin);
		wlSplitEvery.setLayoutData(fdlSplitEvery);
		wSplitEvery=new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wSplitEvery);
		wSplitEvery.addModifyListener(lsMod);
		fdSplitEvery=new FormData();
		fdSplitEvery.left = new FormAttachment(middle, 0);
		fdSplitEvery.top  = new FormAttachment(wFastDump, margin);
		fdSplitEvery.right= new FormAttachment(100, 0);
		wSplitEvery.setLayoutData(fdSplitEvery);





		// SplitFileField Line
		wlSplitFileField=new Label(wContentComp, SWT.RIGHT);
		wlSplitFileField.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.SplitEveryByField.Label")); //$NON-NLS-1$
		props.setLook(wlSplitFileField);
		fdlSplitFileField=new FormData();
		fdlSplitFileField.left = new FormAttachment(0, 0);
		fdlSplitFileField.right= new FormAttachment(middle, -margin);
		fdlSplitFileField.top  = new FormAttachment(wSplitEvery, margin);
		wlSplitFileField.setLayoutData(fdlSplitFileField);

		wSplitFileField=new ComboVar(transMeta, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wSplitFileField);
		wSplitFileField.addModifyListener(lsMod);
		fdSplitFileField=new FormData();
		fdSplitFileField.left = new FormAttachment(middle, 0);
		fdSplitFileField.top  = new FormAttachment(wSplitEvery, margin);
		fdSplitFileField.right= new FormAttachment(100, 0);
		wSplitFileField.setLayoutData(fdSplitFileField);
		wSplitFileField.setEnabled(true);
		wSplitFileField.addFocusListener(new FocusListener()
										 {
											 public void focusLost(org.eclipse.swt.events.FocusEvent e)
											 {
											 }

											 public void focusGained(org.eclipse.swt.events.FocusEvent e)
											 {
												 Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
												 shell.setCursor(busy);
												 getFields2();
												 shell.setCursor(null);
												 busy.dispose();
											 }
										 }
		);






		//Bruise:
		wlEndedLine=new Label(wContentComp, SWT.RIGHT);
		wlEndedLine.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.EndedLine.Label"));
 		props.setLook(wlEndedLine);
		fdlEndedLine=new FormData();
		fdlEndedLine.left = new FormAttachment(0, 0);
		fdlEndedLine.top  = new FormAttachment(wSplitFileField, margin);
		fdlEndedLine.right= new FormAttachment(middle, -margin);
		wlEndedLine.setLayoutData(fdlEndedLine);
		wEndedLine=new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wEndedLine);
 		wEndedLine.addModifyListener(lsMod);
		fdEndedLine=new FormData();
		fdEndedLine.left = new FormAttachment(middle, 0);
		fdEndedLine.top  = new FormAttachment(wSplitFileField, margin);
		fdEndedLine.right= new FormAttachment(100, 0);
		wEndedLine.setLayoutData(fdEndedLine);
		
		fdContentComp = new FormData();
		fdContentComp.left  = new FormAttachment(0, 0);
		fdContentComp.top   = new FormAttachment(0, 0);
		fdContentComp.right = new FormAttachment(100, 0);
		fdContentComp.bottom= new FormAttachment(100, 0);
		wContentComp.setLayoutData(fdContentComp);

		wContentComp.layout();
		wContentTab.setControl(wContentComp);

		/////////////////////////////////////////////////////////////
		/// END OF CONTENT TAB
		/////////////////////////////////////////////////////////////


		// Add listeners
		lsOK       = new Listener() { public void handleEvent(Event e) { ok();       } };
		lsGet      = new Listener() { public void handleEvent(Event e) { get();      } };
		lsMinWidth    = new Listener() { public void handleEvent(Event e) { setMinimalWidth(); } };
		lsCancel   = new Listener() { public void handleEvent(Event e) { cancel();   } };

		/////////////////////////////////////////////////////////////
		// START OF REMOVE TAB
		/////////////////////////////////////////////////////////////
		wRemoveTab=new CTabItem(wTabFolder, SWT.NONE);
		wRemoveTab.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.RemoveTab.TabTitle")); //$NON-NLS-1$

		FormLayout removeLayout = new FormLayout ();
		removeLayout.marginWidth  = margin;
		removeLayout.marginHeight = margin;

		Composite wRemoveComp = new Composite(wTabFolder, SWT.NONE);
		props.setLook(wRemoveComp);
		wRemoveComp.setLayout(removeLayout);

		wlRemove=new Label(wRemoveComp, SWT.NONE);
		wlRemove.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Remove.Label")); //$NON-NLS-1$
		props.setLook(wlRemove);
		fdlRemove=new FormData();
		fdlRemove.left = new FormAttachment(0, 0);
		fdlRemove.top  = new FormAttachment(0, 0);
		wlRemove.setLayoutData(fdlRemove);

		final int RemoveCols=1;
		final int RemoveRows=input.getDeleteName().length;

		ColumnInfo[] colrem=new ColumnInfo[RemoveCols];
		colrem[0]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.ColumnInfo.Fieldname"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]{BaseMessages.getString(PKG, "TextFileOutputDialog.ColumnInfo.Loading")},  false ); //$NON-NLS-1$
		fieldColumns.add(colrem[0]);
		wRemove=new TableView(transMeta, wRemoveComp,
				SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
				colrem,
				RemoveRows,
				lsMod,
				props
		);

		wGetRemove = new Button(wRemoveComp, SWT.PUSH);
		wGetRemove.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.GetRemove.Button")); //$NON-NLS-1$
		wGetRemove.addListener(SWT.Selection, lsGet);
		fdGetRemove = new FormData();
		fdGetRemove.right = new FormAttachment(100, 0);
		fdGetRemove.top   = new FormAttachment(50, 0);
		wGetRemove.setLayoutData(fdGetRemove);

		fdRemove=new FormData();
		fdRemove.left = new FormAttachment(0, 0);
		fdRemove.top  = new FormAttachment(wlRemove, margin);
		fdRemove.right  = new FormAttachment(wGetRemove, -margin);
		fdRemove.bottom = new FormAttachment(100, 0);
		wRemove.setLayoutData(fdRemove);

		fdRemoveComp = new FormData();
		fdRemoveComp.left  = new FormAttachment(0, 0);
		fdRemoveComp.top   = new FormAttachment(0, 0);
		fdRemoveComp.right = new FormAttachment(100, 0);
		fdRemoveComp.bottom= new FormAttachment(100, 0);
		wRemoveComp.setLayoutData(fdRemoveComp);

		wRemoveComp.layout();
		wRemoveTab.setControl(wRemoveComp);

		/////////////////////////////////////////////////////////////
		/// END OF REMOVE TAB
		/////////////////////////////////////////////////////////////



		// Fields tab...
		//
		wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
		wFieldsTab.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FieldsTab.TabTitle"));
		
		FormLayout fieldsLayout = new FormLayout ();
		fieldsLayout.marginWidth  = Const.FORM_MARGIN;
		fieldsLayout.marginHeight = Const.FORM_MARGIN;
		
		Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
		wFieldsComp.setLayout(fieldsLayout);
 		props.setLook(wFieldsComp);

		wGet=new Button(wFieldsComp, SWT.PUSH);
		wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
		wGet.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));

		wMinWidth =new Button(wFieldsComp, SWT.PUSH);
		wMinWidth.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.MinWidth.Button"));
		wMinWidth.setToolTipText(BaseMessages.getString(PKG, "TextFileOutputDialog.MinWidth.Tooltip"));

		setButtonPositions(new Button[] { wGet, wMinWidth}, margin, null);

		final int FieldsCols=10;
		final int FieldsRows=input.getOutputFields().length;
		
		// Prepare a list of possible formats...
		String nums[] = Const.getNumberFormats();
		int totsize = dats.length + nums.length;
		String formats[] = new String[totsize];
		for (int x=0;x<dats.length;x++) formats[x] = dats[x];
		for (int x=0;x<nums.length;x++) formats[dats.length+x] = nums[x];
		
		colinf=new ColumnInfo[FieldsCols];
		colinf[0]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.NameColumn.Column"),       ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false);
		colinf[1]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.TypeColumn.Column"),       ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMeta.getTypes() );
		colinf[2]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.FormatColumn.Column"),     ColumnInfo.COLUMN_TYPE_CCOMBO, formats);
		colinf[3]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.LengthColumn.Column"),     ColumnInfo.COLUMN_TYPE_TEXT,   false);
		colinf[4]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.PrecisionColumn.Column"),  ColumnInfo.COLUMN_TYPE_TEXT,   false);
		colinf[5]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.CurrencyColumn.Column"),   ColumnInfo.COLUMN_TYPE_TEXT,   false);
		colinf[6]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.DecimalColumn.Column"),    ColumnInfo.COLUMN_TYPE_TEXT,   false);
		colinf[7]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.GroupColumn.Column"),      ColumnInfo.COLUMN_TYPE_TEXT,   false);
		colinf[8]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.TrimTypeColumn.Column"),  ColumnInfo.COLUMN_TYPE_CCOMBO,  ValueMeta.trimTypeDesc, true );
		colinf[9]=new ColumnInfo(BaseMessages.getString(PKG, "TextFileOutputDialog.NullColumn.Column"),       ColumnInfo.COLUMN_TYPE_TEXT,   false);
		
		wFields=new TableView(transMeta, wFieldsComp, 
						      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, 
						      colinf, 
						      FieldsRows,  
						      lsMod,
							  props
						      );

		fdFields=new FormData();
		fdFields.left  = new FormAttachment(0, 0);
		fdFields.top   = new FormAttachment(0, 0);
		fdFields.right = new FormAttachment(100, 0);
		fdFields.bottom= new FormAttachment(wGet, -margin);
		wFields.setLayoutData(fdFields);
		
		  // 
        // Search the fields in the background
		
        final Runnable runnable = new Runnable()
        {
            public void run()
            {
                StepMeta stepMeta = transMeta.findStep(stepname);
                if (stepMeta!=null)
                {
                    try
                    {
                    	RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);
                       
                        // Remember these fields...
                        for (int i=0;i<row.size();i++)
                        {
                            inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                        }
                        setComboBoxes();
                    }
                    catch(KettleException e)
                    {
                    	logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
                    }
                }
            }
        };
        new Thread(runnable).start();

		fdFieldsComp=new FormData();
		fdFieldsComp.left  = new FormAttachment(0, 0);
		fdFieldsComp.top   = new FormAttachment(0, 0);
		fdFieldsComp.right = new FormAttachment(100, 0);
		fdFieldsComp.bottom= new FormAttachment(100, 0);
		wFieldsComp.setLayoutData(fdFieldsComp);

		wFieldsComp.layout();
		wFieldsTab.setControl(wFieldsComp);

		fdTabFolder = new FormData();
		fdTabFolder.left  = new FormAttachment(0, 0);
		fdTabFolder.top   = new FormAttachment(wStepname, margin);
		fdTabFolder.right = new FormAttachment(100, 0);
		fdTabFolder.bottom= new FormAttachment(100, -50);
		wTabFolder.setLayoutData(fdTabFolder);
		
		wOK=new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));

		wCancel=new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

		setButtonPositions(new Button[] { wOK, wCancel }, margin, wTabFolder);

		
		wOK.addListener    (SWT.Selection, lsOK    );
		wGet.addListener   (SWT.Selection, lsGet   );
		wMinWidth.addListener (SWT.Selection, lsMinWidth );
		wCancel.addListener(SWT.Selection, lsCancel);

		lsDef=new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };

		wStepname.addSelectionListener( lsDef );
		wFilename.addSelectionListener( lsDef );
		wSeparator.addSelectionListener( lsDef );

		// Whenever something changes, set the tooltip to the expanded version:
		wFilename.addModifyListener(new ModifyListener()
			{
				public void modifyText(ModifyEvent e)
				{
					wFilename.setToolTipText(transMeta.environmentSubstitute( wFilename.getText() ) );
				}
			}
		);
		
		wbFilename.addSelectionListener
		(
			new SelectionAdapter()
			{
				public void widgetSelected(SelectionEvent e) 
				{
					FileDialog dialog = new FileDialog(shell, SWT.SAVE);
					dialog.setFilterExtensions(new String[] {"*.txt", "*.csv", "*"});
					if (wFilename.getText()!=null)
					{
						dialog.setFileName(transMeta.environmentSubstitute(wFilename.getText()));
					}
					dialog.setFilterNames(new String[] {BaseMessages.getString(PKG, "System.FileType.TextFiles"), BaseMessages.getString(PKG, "System.FileType.CSVFiles"), BaseMessages.getString(PKG, "System.FileType.AllFiles")});
					if (dialog.open()!=null)
					{
						String extension = wExtension.getText();
						if ( extension != null && dialog.getFileName() != null &&
								dialog.getFileName().endsWith("." + extension) )
						{
							// The extension is filled in and matches the end 
							// of the selected file => Strip off the extension.
							String fileName = dialog.getFileName();
						    wFilename.setText(dialog.getFilterPath()+System.getProperty("file.separator")+
						    		          fileName.substring(0, fileName.length() - (extension.length()+1)));
						}
						else
						{
						    wFilename.setText(dialog.getFilterPath()+System.getProperty("file.separator")+dialog.getFileName());
						}
					}
				}
			}
		);
		
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(	new ShellAdapter() { public void shellClosed(ShellEvent e) { cancel(); } } );

		lsResize = new Listener() 
		{
			public void handleEvent(Event event) 
			{
				Point size = shell.getSize();
				wFields.setSize(size.x-10, size.y-50);
				wFields.table.setSize(size.x-10, size.y-50);
				wFields.redraw();
			}
		};
		shell.addListener(SWT.Resize, lsResize);

		wTabFolder.setSelection(0);
		
		// Set the shell size, based upon previous time...
		setSize();
		
		getData();
		activeFileNameField();
		enableParentFolder();
		input.setChanged(changed);
		checkPriviledges();		
		shell.open();
		while (!shell.isDisposed())
		{
				if (!display.readAndDispatch()) display.sleep();
		}
		return stepname;
	}
	
	protected void setFlagsServletOption() {
    boolean enableFilename = !wServletOutput.getSelection();
    wlFilename.setEnabled(enableFilename);
    wFilename.setEnabled(enableFilename);
    wlFileIsCommand.setEnabled(enableFilename);
    wFileIsCommand.setEnabled(enableFilename);
    wlDoNotOpenNewFileInit.setEnabled(enableFilename);
    wDoNotOpenNewFileInit.setEnabled(enableFilename);
    wlCreateParentFolder.setEnabled(enableFilename);
    wCreateParentFolder.setEnabled(enableFilename);
    wlExtension.setEnabled(enableFilename);
    wExtension.setEnabled(enableFilename);
    wlSplitEvery.setEnabled(enableFilename);
    wlSplitFileField.setEnabled(enableFilename);
    wSplitEvery.setEnabled(enableFilename);
    wSplitFileField.setEnabled(enableFilename);
    wlAddDate.setEnabled(enableFilename);
    wAddDate.setEnabled(enableFilename);
    wlAddTime.setEnabled(enableFilename);
    wAddTime.setEnabled(enableFilename);
    wlDateTimeFormat.setEnabled(enableFilename);
    wDateTimeFormat.setEnabled(enableFilename);
    wlSpecifyFormat.setEnabled(enableFilename);
    wSpecifyFormat.setEnabled(enableFilename);
    wlAppend.setEnabled(enableFilename);
    wAppend.setEnabled(enableFilename);
    wlAddStepnr.setEnabled(enableFilename);
    wAddStepnr.setEnabled(enableFilename);
    wlAddPartnr.setEnabled(enableFilename);
    wAddPartnr.setEnabled(enableFilename);
    wbShowFiles.setEnabled(enableFilename);
    wlAddToResult.setEnabled(enableFilename);
    wAddToResult.setEnabled(enableFilename);
  }

  private void activeFileNameField()
	{
	   	wlFileNameField.setEnabled(wFileNameInField.getSelection());
	   	wFileNameField.setEnabled(wFileNameInField.getSelection());
//    	wlExtension.setEnabled(!wFileNameInField.getSelection());//can set extension and filename when fileNameInField is enabled
//    	wExtension.setEnabled(!wFileNameInField.getSelection());
//    	wlFilename.setEnabled(!wFileNameInField.getSelection());
//    	wFilename.setEnabled(!wFileNameInField.getSelection());

    	if(wFileNameInField.getSelection()) 
    	{
    		if(!wDoNotOpenNewFileInit.getSelection())
    			wDoNotOpenNewFileInit.setSelection(true);
    		wAddDate.setSelection(false);
    		wAddTime.setSelection(false);
    		wSpecifyFormat.setSelection(false);
    		wAddStepnr.setSelection(false);
    		wAddPartnr.setSelection(false);
    	}
    	
    	wlDoNotOpenNewFileInit.setEnabled(!wFileNameInField.getSelection());
    	wDoNotOpenNewFileInit.setEnabled(!wFileNameInField.getSelection());
    	wlSpecifyFormat.setEnabled(!wFileNameInField.getSelection());
    	wSpecifyFormat.setEnabled(!wFileNameInField.getSelection());
    	
    	wAddStepnr.setEnabled(!wFileNameInField.getSelection());
    	wlAddStepnr.setEnabled(!wFileNameInField.getSelection());
    	wAddPartnr.setEnabled(!wFileNameInField.getSelection());
    	wlAddPartnr.setEnabled(!wFileNameInField.getSelection());
    	if (wFileNameInField.getSelection()) wSplitEvery.setText("0");
    	wSplitEvery.setEnabled(!wFileNameInField.getSelection());
    	wlSplitEvery.setEnabled(!wFileNameInField.getSelection());
		wlSplitFileField.setEnabled(!wFileNameInField.getSelection());
		wSplitFileField.setEnabled(!wFileNameInField.getSelection());
    	if (wFileNameInField.getSelection()) wEndedLine.setText("");    	
      	wEndedLine.setEnabled(!wFileNameInField.getSelection());
      	wbShowFiles.setEnabled(!wFileNameInField.getSelection());
      	wbFilename.setEnabled(!wFileNameInField.getSelection());
      	
      	setDateTimeFormat();
    }
	protected void setComboBoxes()
    {
        // Something was changed in the row.
        //
        final Map<String, Integer> fields = new HashMap<String, Integer>();
        
        // Add the currentMeta fields...
        fields.putAll(inputFields);
        
        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>(keySet);

        String fieldNames[] = (String[]) entries.toArray(new String[entries.size()]);

        Const.sortStrings(fieldNames);
        colinf[0].setComboValues(fieldNames);
		ColumnInfo colInfo = (ColumnInfo) fieldColumns.get(0);
		colInfo.setComboValues(fieldNames);
    }
	private void setDateTimeFormat()
	{
		if(wSpecifyFormat.getSelection())
		{
			wAddDate.setSelection(false);	
			wAddTime.setSelection(false);
		}
		
		wDateTimeFormat.setEnabled(wSpecifyFormat.getSelection() && !wFileNameInField.getSelection());
		wlDateTimeFormat.setEnabled(wSpecifyFormat.getSelection() && !wFileNameInField.getSelection());
		wAddDate.setEnabled(!(wFileNameInField.getSelection() || wSpecifyFormat.getSelection()));
		wlAddDate.setEnabled(!(wSpecifyFormat.getSelection() || wFileNameInField.getSelection()));
		wAddTime.setEnabled(!(wSpecifyFormat.getSelection() || wFileNameInField.getSelection()));
		wlAddTime.setEnabled(!(wSpecifyFormat.getSelection() || wFileNameInField.getSelection()));
	}
    private void setEncodings()
    {
        // Encoding of the text file:
        if (!gotEncodings)
        {
            gotEncodings = true;
            
            wEncoding.removeAll();
            List<Charset> values = new ArrayList<Charset>(Charset.availableCharsets().values());
            for (int i=0;i<values.size();i++)
            {
                Charset charSet = (Charset)values.get(i);
                wEncoding.add( charSet.displayName() );
            }
            
            // Now select the default!
            String defEncoding = Const.getEnvironmentVariable("file.encoding", "UTF-8");
            int idx = Const.indexOfString(defEncoding, wEncoding.getItems() );
            if (idx>=0) wEncoding.select( idx );
        }
    }

    private void getFields()
	 {
		if(!gotPreviousFields)
		{
		 try{
			 String field=wFileNameField.getText();
			 RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			 if(r!=null)
			  {
				 wFileNameField.setItems(r.getFieldNames());
			  }
			 if(field!=null) wFileNameField.setText(field);
		 	}catch(KettleException ke){
				new ErrorDialog(shell, BaseMessages.getString(PKG, "TextFileOutputDialog.FailedToGetFields.DialogTitle"), BaseMessages.getString(PKG, "TextFileOutputDialog.FailedToGetFields.DialogMessage"), ke);
			}
		 	gotPreviousFields=true;
		}
	 }

	private void getFields2()
	{
		if(!gotPreviousFields2)
		{
			try{
				String field=wSplitFileField.getText();
				RowMetaInterface r = transMeta.getPrevStepFields(stepname);
				if(r!=null)
				{
					wSplitFileField.setItems(r.getFieldNames());
				}
				if(field!=null) wSplitFileField.setText(field);
			}catch(KettleException ke){
				new ErrorDialog(shell, BaseMessages.getString(PKG, "TextFileOutputDialog.FailedToGetFields.DialogTitle"), BaseMessages.getString(PKG, "TextFileOutputDialog.FailedToGetFields.DialogMessage"), ke);
			}
			gotPreviousFields2=true;
		}
	}
    /**
	 * Copy information from the meta-data input to the dialog fields.
	 */ 
	public void getData()
	{
		if (input.getFileName()  != null) wFilename.setText(input.getFileName());
		wFileIsCommand.setSelection(input.isFileAsCommand());
		wServletOutput.setSelection(input.isServletOutput());
		setFlagsServletOption();
		wDoNotOpenNewFileInit.setSelection(input.isDoNotOpenNewFileInit());
		wCreateParentFolder.setSelection(input.isCreateParentFolder());
		wExtension.setText(Const.NVL(input.getExtension(), ""));
		wSeparator.setText(Const.NVL(input.getSeparator(), ""));
		wSeparatorAfterLastColumn.setSelection(input.isWriteSepatatorAfterLastColumn());//jason 2016
		wRemoveCRLF.setSelection(input.isRemoveCRLF());//jason 2016
		wEnclosure.setText(Const.NVL(input.getEnclosure(), ""));
		
		if (input.getFileFormat()!=null) {
			wFormat.select(0); // default if not found: CR+LF
			for (int i=0;i<TextFileOutputMeta.formatMapperLineTerminator.length;i++) {
				if(input.getFileFormat().equals(TextFileOutputMeta.formatMapperLineTerminator[i])) wFormat.select(i);
			}
		}
		if (input.getFileCompression()!=null) wCompression.setText(input.getFileCompression());
        if (input.getEncoding()  !=null) wEncoding.setText(input.getEncoding());
        if (input.getEndedLine() !=null) wEndedLine.setText(input.getEndedLine());
        
        wFileNameInField.setSelection(input.isFileNameInField());
        if (input.getFileNameField() !=null) wFileNameField.setText(input.getFileNameField());
 		if(input.getSplitFileField() != null ){wSplitFileField.setText(input.getSplitFileField());}

		wSplitEvery.setText(""+input.getSplitEvery());

    wEnclForced.setSelection(input.isEnclosureForced());
    wDisableEnclosureFix.setSelection(input.isEnclosureFixDisabled());
		wHeader.setSelection(input.isHeaderEnabled());
		wFooter.setSelection(input.isFooterEnabled());
		wAddDate.setSelection(input.isDateInFilename());
		wAddTime.setSelection(input.isTimeInFilename());
		wDateTimeFormat.setText(Const.NVL(input.getDateTimeFormat(), ""));
		wSpecifyFormat.setSelection(input.isSpecifyingFormat());

		wAppend.setSelection(input.isFileAppended());
		wAddStepnr.setSelection(input.isStepNrInFilename());
		wAddPartnr.setSelection(input.isPartNrInFilename());
		wPad.setSelection(input.isPadded());
		wFastDump.setSelection(input.isFastDump());
		wAddToResult.setSelection(input.isAddToResultFiles());

		logDebug("getting Removes info...");
		/*Remove
		 * Remove certain fields...
		 */
		if (input.getDeleteName()!=null && input.getDeleteName().length>0)
		{
			for (int i=0;i<input.getDeleteName().length;i++)
			{
				TableItem item = wRemove.table.getItem(i);
				if (input.getDeleteName()[i]!=null)  item.setText(1, input.getDeleteName()     [i]);
			}
			wRemove.setRowNums();
			wRemove.optWidth(true);
			wTabFolder.setSelection(1);
		}

				
		logDebug("getting fields info...");
		
		for (int i=0;i<input.getOutputFields().length;i++)
		{
		    TextFileField field = input.getOutputFields()[i];
		    
			TableItem item = wFields.table.getItem(i);
			if (field.getName()!=null) item.setText(1, field.getName());
			item.setText(2, field.getTypeDesc());
			if (field.getFormat()!=null) item.setText(3, field.getFormat());
			if (field.getLength()>=0) item.setText(4, ""+field.getLength());
			if (field.getPrecision()>=0) item.setText(5, ""+field.getPrecision());
			if (field.getCurrencySymbol()!=null) item.setText(6, field.getCurrencySymbol());
			if (field.getDecimalSymbol()!=null) item.setText(7, field.getDecimalSymbol());
			if (field.getGroupingSymbol()!=null) item.setText(8, field.getGroupingSymbol());
			String trim = field.getTrimTypeDesc();
			if (trim != null) item.setText(9, trim);
			if (field.getNullString()!=null) item.setText(10, field.getNullString());
		}
		
		wFields.optWidth(true);
		wStepname.selectAll();
	}
	
	private void cancel()
	{
		stepname=null;
		
		input.setChanged(backupChanged);

		dispose();
	}
	
	private void getInfo(TextFileOutputMeta tfoi)
	{
		tfoi.setFileName(   wFilename.getText() );
		tfoi.setFileAsCommand( wFileIsCommand.getSelection() );
		tfoi.setServletOutput(wServletOutput.getSelection() );
		tfoi.setCreateParentFolder(wCreateParentFolder.getSelection() );
		tfoi.setDoNotOpenNewFileInit(wDoNotOpenNewFileInit.getSelection() );
		tfoi.setFileFormat( TextFileOutputMeta.formatMapperLineTerminator[wFormat.getSelectionIndex()] );
		tfoi.setFileCompression( wCompression.getText() );
        tfoi.setEncoding( wEncoding.getText() );
		tfoi.setSeparator(  wSeparator.getText() );
		tfoi.setEnclosure(  wEnclosure.getText() );
		tfoi.setExtension(  wExtension.getText() );
		tfoi.setSplitEvery( Const.toInt(wSplitEvery.getText(), 0) );
		tfoi.setEndedLine( wEndedLine.getText() );
		tfoi.setWriteSepatatorAfterLashColumn(wSeparatorAfterLastColumn.getSelection());//jason 2016
		tfoi.setRemoveCRLF(wRemoveCRLF.getSelection());//jason

		tfoi.setFileNameField(   wFileNameField.getText() );
		tfoi.setFileNameInField( wFileNameInField.getSelection() );

        tfoi.setEnclosureForced( wEnclForced.getSelection() );
        tfoi.setEnclosureFixDisabled( wDisableEnclosureFix.getSelection() );
		tfoi.setHeaderEnabled( wHeader.getSelection() ); 
		tfoi.setFooterEnabled( wFooter.getSelection() );
		tfoi.setFileAppended( wAppend.getSelection() );
		tfoi.setStepNrInFilename( wAddStepnr.getSelection() );
		tfoi.setPartNrInFilename( wAddPartnr.getSelection() );
		tfoi.setSplitFileField(wSplitFileField.getText());

		tfoi.setDateInFilename( wAddDate.getSelection() );
		tfoi.setTimeInFilename( wAddTime.getSelection() );
		tfoi.setDateTimeFormat(wDateTimeFormat.getText());
		tfoi.setSpecifyingFormat(wSpecifyFormat.getSelection());
		tfoi.setPadded( wPad.getSelection() );
		tfoi.setAddToResultFiles( wAddToResult.getSelection() );
		tfoi.setFastDump( wFastDump.getSelection() );

		int i;
		//Table table = wFields.table;
		
		int nrfields = wFields.nrNonEmpty();
		int nrremove = wRemove.nrNonEmpty();



		tfoi.allocate(nrremove,nrfields);
		
		for (i=0;i<nrfields;i++)
		{
		    TextFileField field = new TextFileField();
		    
			TableItem item = wFields.getNonEmpty(i);
			field.setName( item.getText(1) );
			field.setType( item.getText(2) );
			field.setFormat( item.getText(3) );
			field.setLength( Const.toInt(item.getText(4), -1) );
			field.setPrecision( Const.toInt(item.getText(5), -1) );
			field.setCurrencySymbol( item.getText(6) );
			field.setDecimalSymbol( item.getText(7) );
			field.setGroupingSymbol( item.getText(8) );
			field.setTrimType( ValueMeta.getTrimTypeByDesc(item.getText(9)));
			field.setNullString( item.getText(10) );
			tfoi.getOutputFields()[i]  = field;
		}

		for (i=0;i<nrremove;i++)
		{
			TableItem item = wRemove.getNonEmpty(i);
			tfoi.getDeleteName()  [i] = item.getText(1);
		}
	}
	
	private void ok()
	{


		if (Const.isEmpty(wStepname.getText())) return;

		stepname = wStepname.getText(); // return value
		
		getInfo(input);
		
		dispose();
	}
	
	private void get()
	{
		try
		{
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r!=null)
			{
				switch (wTabFolder.getSelectionIndex())
				{
					case 2 :
						BaseStepDialog.getFieldsFromPrevious(r, wRemove, 1, new int[] { 1 }, new int[] {}, -1, -1, null); break;
					case 3 :
						TableItemInsertListener listener = new TableItemInsertListener()
						{
							public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v)
							{
								if (v.isNumber())
								{
									if (v.getLength()>0)
									{
										int le=v.getLength();
										int pr=v.getPrecision();

										if (v.getPrecision()<=0)
										{
											pr=0;
										}

										String mask="";
										for (int m=0;m<le-pr;m++)
										{
											mask+="0";
										}
										if (pr>0) mask+=".";
										for (int m=0;m<pr;m++)
										{
											mask+="0";
										}
										tableItem.setText(3, mask);
									}
								}
								return true;
							}
						};
						BaseStepDialog.getFieldsFromPrevious(r, wFields, 1, new int[] { 1 }, new int[] { 2 }, 4, 5, listener);
						break;
				}


            }
		}
		catch(KettleException ke)
		{
			new ErrorDialog(shell, BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"), BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"), ke);
		}

	}
	
	/**
	 * Sets the output width to minimal width...
	 *
	 */
	public void setMinimalWidth()
	{
    	int nrNonEmptyFields = wFields.nrNonEmpty(); 
		for (int i=0;i<nrNonEmptyFields;i++)
		{
			TableItem item = wFields.getNonEmpty(i);
			
			item.setText(4, "");
			item.setText(5, "");
			item.setText(9, ValueMeta.getTrimTypeDesc(ValueMetaInterface.TRIM_TYPE_BOTH));
			
			int type = ValueMeta.getType(item.getText(2));
			switch(type)
			{
			case ValueMetaInterface.TYPE_STRING:  item.setText(3, ""); break;
			case ValueMetaInterface.TYPE_INTEGER: item.setText(3, "0"); break;
			case ValueMetaInterface.TYPE_NUMBER: item.setText(3, "0.#####"); break;
			case ValueMetaInterface.TYPE_DATE: break;
			default: break;
			}
		}
		
		for (int i=0;i<input.getOutputFields().length;i++)
			input.getOutputFields()[i].setTrimType(ValueMetaInterface.TRIM_TYPE_BOTH);
				
		wFields.optWidth(true);
	}
	private void enableParentFolder()
	{
		wlCreateParentFolder.setEnabled(!wFileIsCommand.getSelection());
		wCreateParentFolder.setEnabled(!wFileIsCommand.getSelection());
	}
}
