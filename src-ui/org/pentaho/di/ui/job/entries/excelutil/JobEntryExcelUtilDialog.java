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

/*
 * Created on 19-jun-2003
 *
 */

package org.pentaho.di.ui.job.entries.excelutil;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox; 
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.evaluatetablecontent.JobEntryEvalTableContent;
import org.pentaho.di.job.entries.excelutil.JobEntryExcelUtil;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog; 
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * This dialog allows you to edit the SQL job entry settings. (select the connection and the sql
 * script to be executed)
 * 
 * @author Matt
 * @since 19-06-2003
 */
public class JobEntryExcelUtilDialog extends JobEntryDialog implements JobEntryDialogInterface
{
    private static Class<?> PKG = JobEntryExcelUtil.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    private Label wlName;
    private Text wName;
    private FormData fdlName, fdName;

   
    private Label wlTargetFile;
    private TextVar wTargetFile;
    private FormData fdlTargetFile, fdTargetFile;
    

	private Label wlSourceFiles;
	private TableView wAppendElements;
	private FormData fdlAppendElements, fdAppendElements;

	private Label wlAddToResult;
	private Button wAddToResult;
	private FormData fdlAddToResult, fdAddToResult;
	
    private Button wOK, wCancel;
    private Listener lsOK, lsCancel;

    private JobEntryExcelUtil jobEntry;

    private Shell shell;

    private SelectionAdapter lsDef;

    private boolean changed;
	
    public JobEntryExcelUtilDialog(Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta)
    {
        super(parent, jobEntryInt, rep, jobMeta);
        jobEntry = (JobEntryExcelUtil) jobEntryInt;
        if (this.jobEntry.getName() == null)
            this.jobEntry.setName(BaseMessages.getString(PKG, "ExcelUtil.Name.Default"));
    }

    public JobEntryInterface open()
    {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, props.getJobsDialogStyle());
        props.setLook(shell);
        JobDialog.setShellImage(shell, jobEntry);

        ModifyListener lsMod = new ModifyListener()
        {
            public void modifyText(ModifyEvent e)
            {
                jobEntry.setChanged();
            }
        };
        changed = jobEntry.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "ExcelUtil.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Job entry name line
        wlName = new Label(shell, SWT.RIGHT);
        wlName.setText(BaseMessages.getString(PKG, "ExcelUtil.Name.Label"));
        props.setLook(wlName);
        fdlName = new FormData();
        fdlName.left = new FormAttachment(0, 0);
        fdlName.right = new FormAttachment(middle, -margin);
        fdlName.top = new FormAttachment(0, margin);
        wlName.setLayoutData(fdlName);
        wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wName);
        wName.addModifyListener(lsMod);
        fdName = new FormData();
        fdName.left = new FormAttachment(middle, 0);
        fdName.top = new FormAttachment(0, margin);
        fdName.right = new FormAttachment(100, 0);
        wName.setLayoutData(fdName);

        
        // TargetFile line
        wlTargetFile = new Label(shell, SWT.RIGHT);
        wlTargetFile.setText(BaseMessages.getString(PKG, "ExcelUtil.TargetFile.Label"));
        props.setLook(wlTargetFile);
        fdlTargetFile = new FormData();
        fdlTargetFile.left = new FormAttachment(0, 0);
        fdlTargetFile.top = new FormAttachment(wName, margin);
        fdlTargetFile.right = new FormAttachment(middle, -margin);
        wlTargetFile.setLayoutData(fdlTargetFile);
        wTargetFile = new TextVar(jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wTargetFile);
        wTargetFile.setToolTipText(BaseMessages.getString(PKG, "ExcelUtil.TargetFile.Tooltip"));
        wTargetFile.addModifyListener(lsMod);
        fdTargetFile = new FormData();
        fdTargetFile.left = new FormAttachment(middle, 0);
        fdTargetFile.top = new FormAttachment(wName, margin);
        fdTargetFile.right = new FormAttachment(100, 0);
        wTargetFile.setLayoutData(fdTargetFile);

        
		wlSourceFiles = new Label(shell, SWT.NONE);
		wlSourceFiles.setText(BaseMessages.getString(PKG, "ExcelUtil.SourceFiles.Label"));
		props.setLook(wlSourceFiles);
		fdlAppendElements = new FormData();
		fdlAppendElements.left = new FormAttachment(0, 0);
		fdlAppendElements.top = new FormAttachment(wTargetFile, margin);
		wlSourceFiles.setLayoutData(fdlAppendElements);

		final int FieldsCols = 1;
		int pathsRows;
		if (jobEntry.getSourceFileName() == null)
			pathsRows = 5;
		else	
		    pathsRows = jobEntry.getSourceFileName().length;
		ColumnInfo[] cols = new ColumnInfo[FieldsCols];
		cols[0] = new ColumnInfo(BaseMessages.getString(PKG, "ExcelUtil.Path.Column"),
				ColumnInfo.COLUMN_TYPE_TEXT, false);

		wAppendElements = new TableView( jobEntry, shell, SWT.BORDER
				| SWT.FULL_SELECTION | SWT.MULTI, cols, pathsRows, lsMod, props);

		fdAppendElements = new FormData();
		fdAppendElements.left = new FormAttachment(0, 0);
		fdAppendElements.top = new FormAttachment(wlSourceFiles, margin);
		fdAppendElements.right = new FormAttachment(100, 0);
		fdAppendElements.bottom = new FormAttachment(80, -2 * margin);
		wAppendElements.setLayoutData(fdAppendElements);

		// Add File to the result files name
		wlAddToResult = new Label(shell, SWT.RIGHT);
		wlAddToResult.setText(BaseMessages.getString(PKG, "ExcelUtil.AddFileToResult.Label"));
		props.setLook(wlAddToResult);
		fdlAddToResult = new FormData();
		fdlAddToResult.left = new FormAttachment(0, 0);
		fdlAddToResult.top = new FormAttachment(wAppendElements, 2 * margin);
		fdlAddToResult.right = new FormAttachment(middle, -margin);
		wlAddToResult.setLayoutData(fdlAddToResult);
		wAddToResult = new Button(shell, SWT.CHECK);
		wAddToResult.setToolTipText(BaseMessages.getString(PKG, "ExcelUtil.AddFileToResult.Tooltip"));
		props.setLook(wAddToResult);
		fdAddToResult = new FormData();
		fdAddToResult.left = new FormAttachment(middle, 0);
		fdAddToResult.top = new FormAttachment(wAppendElements, 2 * margin);
		fdAddToResult.right = new FormAttachment(100, 0);
		wAddToResult.setLayoutData(fdAddToResult);
		SelectionAdapter lsSelR = new SelectionAdapter() {
			public void widgetSelected(SelectionEvent arg0) {
				jobEntry.setChanged();
			}
		};
		wAddToResult.addSelectionListener(lsSelR);
		
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

        BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel }, margin,
        		wAddToResult);

        // Add listeners
        lsCancel = new Listener()
        {
            public void handleEvent(Event e)
            {
                cancel();
            }
        };
        lsOK = new Listener()
        {
            public void handleEvent(Event e)
            {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

        lsDef = new SelectionAdapter()
        {
            public void widgetDefaultSelected(SelectionEvent e)
            {
                ok();
            }
        };

        wName.addSelectionListener(lsDef);
        wTargetFile.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter()
        {
            public void shellClosed(ShellEvent e)
            {
                cancel();
            }
        });

        getData();

        BaseStepDialog.setSize(shell);

        shell.open();
        props.setDialogSize(shell, "JobExcelUtilDialogSize");
        while (!shell.isDisposed())
        {
            if (!display.readAndDispatch())
                display.sleep();
        }
        return jobEntry;
    }



    public void dispose()
    {
        WindowProperty winprop = new WindowProperty(shell);
        props.setScreen(winprop);
        shell.dispose();
    }

    /**
     * Copy information from the meta-data input to the dialog fields.
     */
    public void getData()
    {
        if (jobEntry.getName() != null)
            wName.setText(jobEntry.getName());
        wName.selectAll();
        wTargetFile.setText(Const.NVL(jobEntry.getTargetFilename(), ""));
        if (jobEntry.getSourceFileName()!=null)
        {
		for (int i = 0; i < jobEntry.getSourceFileName().length; i++) {
			TableItem item = wAppendElements.table.getItem(i);
			if (jobEntry.getSourceFileName()[i] != null)
				item.setText(1, jobEntry.getSourceFileName()[i]);
		}
        }
        wAddToResult.setSelection( jobEntry.isAddToResult());

    }

    private void cancel()
    {
        jobEntry.setChanged(changed);
        jobEntry = null;
        dispose();
    }

    private void ok()
    {
 	   if(Const.isEmpty(wName.getText())) 
       {
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR );
			mb.setText(BaseMessages.getString(PKG, "System.StepJobEntryNameMissing.Title"));
			mb.setMessage(BaseMessages.getString(PKG, "System.JobEntryNameMissing.Msg"));
			mb.open(); 
			return;
       }
        jobEntry.setName(wName.getText());
        jobEntry.setTargetFilename(wTargetFile.getText());
        jobEntry.setAddToResult(wAddToResult.getSelection());
		int nrElements = wAppendElements.nrNonEmpty();
		jobEntry.allocate( nrElements);
        for (int i = 0; i < nrElements; i++) {
			TableItem item = wAppendElements.getNonEmpty(i);
			jobEntry.getSourceFileName()[i] = item.getText(1);
		}
        
        
        dispose();
    }

    public String toString()
    {
        return this.getClass().getName();
    }

    public boolean evaluates()
    {
        return true;
    }

    public boolean isUnconditional()
    {
        return false;
    }

}
