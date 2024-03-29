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

package org.pentaho.di.ui.trans.dialog;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class TransExecutionConfigurationDialog extends Dialog
{
    private static Class<?> PKG = TransDialog.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    private Display display;
    private Shell parent;
    private Shell shell;
    private PropsUI props;
    private boolean retval;
    
    private Button wOK, wCancel;
    
    private Group gLocal;
    
    private TransExecutionConfiguration configuration;
    private TransMeta transMeta;

    private Button wExecLocal;
    private Button wExecRemote;
    private Button wExecCluster;
    private Button wSafeMode;
    private Button wClearLog;
    private Button wPrepareExecution;
    private Button wPostTransformation;
    private Button wStartExecution;
    private Button wShowTransformations;
    private CCombo wRemoteHost;
    private Label wlRemoteHost;
    private Text wReplayDate;
    private TableView wArguments;
    private Label wlArguments;
    private TableView wParams;
    private Label wlParams;
    private Label wlVariables;
    private TableView wVariables;
    private Label wlReplayDate;
    private Label wlLogLevel;
    private CCombo wLogLevel;

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	private Group gDetails;
	private Button	wPassExport;

    public TransExecutionConfigurationDialog(Shell parent, TransExecutionConfiguration configuration, TransMeta transMeta)
    {
        super(parent);
        this.parent = parent;
        this.configuration = configuration;
        this.transMeta  = transMeta;
        
        // Fill the parameters, maybe do this in another place?
        Map<String, String> params = configuration.getParams();
        params.clear();
        String[] paramNames = transMeta.listParameters();
        for ( String name : paramNames ) {
        	params.put(name, "");
        }
                
        props = PropsUI.getInstance();
    }
    
    public boolean open()
    {
        display = parent.getDisplay();
        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        props.setLook(shell);
		shell.setImage(GUIResource.getInstance().getImageTransGraph());
        
        FormLayout formLayout = new FormLayout ();
        formLayout.marginWidth  = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.Shell.Title")); //$NON-NLS-1$

        int margin = Const.MARGIN;
        int tabsize = 5*margin;
        
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.Button.Launch"));
        wOK.addSelectionListener(new SelectionAdapter() { public void widgetSelected(SelectionEvent e) { ok(); }});
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
        wCancel.addSelectionListener(new SelectionAdapter() { public void widgetSelected(SelectionEvent e) { cancel(); }});
        
        BaseStepDialog.positionBottomButtons(shell, new Button[] { wOK, wCancel }, margin, null);
        
        
        gLocal = new Group(shell, SWT.SHADOW_ETCHED_IN);
        gLocal.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.LocalGroup.Label")); //$NON-NLS-1$;
        // The layout
        FormLayout localLayout = new FormLayout();
        localLayout.marginWidth  = Const.FORM_MARGIN;
        localLayout.marginHeight = Const.FORM_MARGIN;
        gLocal.setLayout(localLayout);
        // 
        FormData fdLocal=new FormData();
        fdLocal.left   = new FormAttachment(0, 0);
        fdLocal.right  = new FormAttachment(100, 0);
        gLocal.setBackground(shell.getBackground()); // the default looks ugly
        gLocal.setLayoutData(fdLocal);

        /////////////////////////////////////////////////////////////////////////////////////////////////
        // Local execution
        //
        wExecLocal=new Button(gLocal, SWT.RADIO);
        wExecLocal.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ExecLocal.Label")); //$NON-NLS-1$
        wExecLocal.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ExecLocal.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wExecLocal);
        FormData fdExecLocal = new FormData();
        fdExecLocal.left  = new FormAttachment(0, 0);
        fdExecLocal.right = new FormAttachment(33, 0);
        wExecLocal.setLayoutData(fdExecLocal);
        wExecLocal.addSelectionListener(new SelectionAdapter() { public void widgetSelected(SelectionEvent e) { enableFields(); }});
        
        /////////////////////////////////////////////////////////////////////////////////////////////////
        // remote execution
        //
        wExecRemote=new Button(gLocal, SWT.RADIO);
        wExecRemote.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ExecRemote.Label")); //$NON-NLS-1$
        wExecRemote.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ExecRemote.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wExecRemote);
        FormData fdExecRemote = new FormData();
        fdExecRemote.left  = new FormAttachment(33, margin);
        fdExecRemote.right = new FormAttachment(66, 0);
        wExecRemote.setLayoutData(fdExecRemote);
        wExecRemote.addSelectionListener(new SelectionAdapter() { public void widgetSelected(SelectionEvent e) { enableFields(); }});

        wlRemoteHost = new Label(gLocal, SWT.LEFT);
        props.setLook(wlRemoteHost);
        wlRemoteHost.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.RemoteHost.Label")); //$NON-NLS-1$
        wlRemoteHost.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.RemoteHost.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        FormData fdlRemoteHost = new FormData();
        fdlRemoteHost.left  = new FormAttachment(33, tabsize);
        fdlRemoteHost.top   = new FormAttachment(wExecRemote, margin*2);
        wlRemoteHost.setLayoutData(fdlRemoteHost);

        wRemoteHost = new CCombo(gLocal, SWT.READ_ONLY | SWT.BORDER);
        wRemoteHost.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.RemoteHost.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wRemoteHost);
        FormData fdRemoteHost = new FormData();
        fdRemoteHost.left  = new FormAttachment(wlRemoteHost, margin);
        fdRemoteHost.right = new FormAttachment(66, 0);
        fdRemoteHost.top   = new FormAttachment(wExecRemote, margin*2);
        wRemoteHost.setLayoutData(fdRemoteHost);
        for (int i=0;i<transMeta.getSlaveServers().size();i++)
        {
            SlaveServer slaveServer = (SlaveServer)transMeta.getSlaveServers().get(i);
            wRemoteHost.add(slaveServer.toString());
        }
        
        wPassExport = new Button(gLocal, SWT.CHECK);
        wPassExport.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.PassExport.Label")); //$NON-NLS-1$
        wPassExport.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.PassExport.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wPassExport);
        FormData fdPassExport = new FormData();
        fdPassExport.left  = new FormAttachment(33, margin);
        fdPassExport.top   = new FormAttachment(wRemoteHost, margin);
        wPassExport.setLayoutData(fdPassExport);

        /////////////////////////////////////////////////////////////////////////////////////////////////
        // Clustered execution
        //
        wExecCluster=new Button(gLocal, SWT.RADIO);
        wExecCluster.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ExecCluster.Label")); //$NON-NLS-1$
        wExecCluster.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ExecCluster.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wExecCluster);
        FormData fdExecCluster = new FormData();
        fdExecCluster.left  = new FormAttachment(66, margin);
        fdExecCluster.right = new FormAttachment(100, 0);
        wExecCluster.setLayoutData(fdExecCluster);
        wExecCluster.addSelectionListener(new SelectionAdapter() { public void widgetSelected(SelectionEvent e) { enableFields(); }});

        wPostTransformation = new Button(gLocal, SWT.CHECK);
        wPostTransformation.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.PostTransformation.Label")); //$NON-NLS-1$
        wPostTransformation.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.PostTransformation.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wPostTransformation);
        FormData fdPostTransformation = new FormData();
        fdPostTransformation.left  = new FormAttachment(66, tabsize);
        fdPostTransformation.right = new FormAttachment(100, 0);
        fdPostTransformation.top   = new FormAttachment(wExecCluster, margin*2);
        wPostTransformation.setLayoutData(fdPostTransformation);

        wPrepareExecution = new Button(gLocal, SWT.CHECK);
        wPrepareExecution.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.PrepareExecution.Label")); //$NON-NLS-1$
        wPrepareExecution.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.PrepareExecution.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wPrepareExecution);
        FormData fdPrepareExecution = new FormData();
        fdPrepareExecution.left  = new FormAttachment(66, tabsize);
        fdPrepareExecution.right = new FormAttachment(100, 0);
        fdPrepareExecution.top   = new FormAttachment(wPostTransformation, margin);
        wPrepareExecution.setLayoutData(fdPrepareExecution);
        
        wStartExecution = new Button(gLocal, SWT.CHECK);
        wStartExecution.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.StartExecution.Label")); //$NON-NLS-1$
        wStartExecution.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.StartExecution.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wStartExecution);
        FormData fdStartExecution = new FormData();
        fdStartExecution.left  = new FormAttachment(66, tabsize);
        fdStartExecution.right = new FormAttachment(100, 0);
        fdStartExecution.top   = new FormAttachment(wPrepareExecution, margin);
        wStartExecution.setLayoutData(fdStartExecution);

        wShowTransformations = new Button(gLocal, SWT.CHECK);
        wShowTransformations.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ShowTransformations.Label")); //$NON-NLS-1$
        wShowTransformations.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ShowTransformations.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wShowTransformations);
        FormData fdShowTransformations = new FormData();
        fdShowTransformations.left  = new FormAttachment(66, tabsize);
        fdShowTransformations.right = new FormAttachment(100, 0);
        fdShowTransformations.top   = new FormAttachment(wStartExecution, margin);
        wShowTransformations.setLayoutData(fdShowTransformations);

        /////////////////////////////////////////////////////////////////////////////////////////////////
        // Replay date, arguments & variables
        //
        
        gDetails = new Group(shell, SWT.SHADOW_ETCHED_IN);
        gDetails.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.DetailsGroup.Label")); //$NON-NLS-1$;
        // The layout
        FormLayout detailsLayout = new FormLayout();
        detailsLayout.marginWidth  = Const.FORM_MARGIN;
        detailsLayout.marginHeight = Const.FORM_MARGIN;
        gDetails.setLayout(detailsLayout);
        // 
        FormData fdDetails=new FormData();
        fdDetails.left   = new FormAttachment(0, 0);
        fdDetails.top    = new FormAttachment(gLocal, margin*2);
        fdDetails.right  = new FormAttachment(100, 0);
        gDetails.setBackground(shell.getBackground()); // the default looks ugly
        gDetails.setLayoutData(fdDetails);

        wSafeMode = new Button(gDetails, SWT.CHECK);
        wSafeMode.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.SafeMode.Label")); //$NON-NLS-1$
        wSafeMode.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.SafeMode.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wSafeMode);
        FormData fdSafeMode = new FormData();
        fdSafeMode.left  = new FormAttachment( 50, margin);
        fdSafeMode.right = new FormAttachment(100, 0);
        fdSafeMode.top   = new FormAttachment(0, 0);
        wSafeMode.setLayoutData(fdSafeMode);
        wSafeMode.addSelectionListener(new SelectionAdapter() { public void widgetSelected(SelectionEvent e) { enableFields(); }});

        wClearLog = new Button(gDetails, SWT.CHECK);
        wClearLog.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ClearLog.Label")); //$NON-NLS-1$
        wClearLog.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ClearLog.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wClearLog);
        FormData fdClearLog = new FormData();
        fdClearLog.left  = new FormAttachment( 50, margin);
        fdClearLog.right = new FormAttachment(100, 0);
        fdClearLog.top   = new FormAttachment(wSafeMode, margin);
        wClearLog.setLayoutData(fdClearLog);
        wClearLog.addSelectionListener(new SelectionAdapter() { public void widgetSelected(SelectionEvent e) { enableFields(); }});

        wlLogLevel = new Label(gDetails, SWT.LEFT);
        props.setLook(wlLogLevel);
        wlLogLevel.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.LogLevel.Label")); //$NON-NLS-1$
        wlLogLevel.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.LogLevel.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        FormData fdlLogLevel = new FormData();
        fdlLogLevel.left  = new FormAttachment(0, 0);
        fdlLogLevel.right = new FormAttachment(50, 0);
        fdlLogLevel.top   = new FormAttachment(wClearLog, margin);
        wlLogLevel.setLayoutData(fdlLogLevel);

        wLogLevel = new CCombo(gDetails, SWT.READ_ONLY | SWT.BORDER);
        wLogLevel.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.LogLevel.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        props.setLook(wLogLevel);
        FormData fdLogLevel = new FormData();
        fdLogLevel.left  = new FormAttachment(50, margin);
        fdLogLevel.right = new FormAttachment(100, 0);
        fdLogLevel.top   = new FormAttachment(wClearLog, margin);
        wLogLevel.setLayoutData(fdLogLevel);
        wLogLevel.setItems(LogLevel.getLogLevelDescriptions());

        // ReplayDate
        wlReplayDate = new Label(gDetails, SWT.LEFT);
        props.setLook(wlReplayDate);
        wlReplayDate.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ReplayDate.Label")); //$NON-NLS-1$
        wlReplayDate.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ReplayDate.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        FormData fdlReplayDate = new FormData();
        fdlReplayDate.left   = new FormAttachment(0, 0);
        fdlReplayDate.right  = new FormAttachment(50, 0);
        fdlReplayDate.top    = new FormAttachment(wLogLevel, margin);
        wlReplayDate.setLayoutData(fdlReplayDate);

        wReplayDate = new Text(gDetails, SWT.LEFT | SWT.BORDER | SWT.SINGLE);
        props.setLook(wReplayDate);
        wReplayDate.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ReplayDate.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        FormData fdReplayDate = new FormData();
        fdReplayDate.left   = new FormAttachment(50, margin);
        fdReplayDate.right  = new FormAttachment(100, 0);
        fdReplayDate.top    = new FormAttachment(wLogLevel, margin);
        wReplayDate.setLayoutData(fdReplayDate);
      
        // Variables
        wlVariables = new Label(shell, SWT.LEFT);
        props.setLook(wlVariables);
        wlVariables.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.Variables.Label")); //$NON-NLS-1$
        wlVariables.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.Variables.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        FormData fdlVariables = new FormData();
        fdlVariables.left   = new FormAttachment(50, margin);
        fdlVariables.right  = new FormAttachment(100, 0);
        fdlVariables.top    = new FormAttachment(gDetails, margin*2);
        wlVariables.setLayoutData(fdlVariables);

        ColumnInfo[] cVariables = {
            new ColumnInfo( BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.VariablesColumn.Argument"), ColumnInfo.COLUMN_TYPE_TEXT, false, false), //Stepname
            new ColumnInfo( BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.VariablesColumn.Value"), ColumnInfo.COLUMN_TYPE_TEXT, false, false), //Preview size
          };
              
        int nrVariables = configuration.getVariables() !=null ? configuration.getVariables().size() : 0; 
        wVariables = new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, cVariables, nrVariables, false, null, props);
        FormData fdVariables = new FormData();
        fdVariables.left   = new FormAttachment(50, margin);
        fdVariables.right  = new FormAttachment(100, 0);
        fdVariables.top    = new FormAttachment(wlVariables, margin);
        fdVariables.bottom = new FormAttachment(wOK, -margin*2);
        wVariables.setLayoutData(fdVariables);        
        
        // Arguments
        wlArguments = new Label(shell, SWT.LEFT);
        props.setLook(wlArguments);
        wlArguments.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.Arguments.Label")); //$NON-NLS-1$
        wlArguments.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.Arguments.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        FormData fdlArguments = new FormData();
        fdlArguments.left   = new FormAttachment(0, 0);
        fdlArguments.right  = new FormAttachment(50, -margin);
        fdlArguments.top    = new FormAttachment(wVariables, 0, SWT.CENTER);
        wlArguments.setLayoutData(fdlArguments);

        ColumnInfo[] cArguments = {
            new ColumnInfo( BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ArgumentsColumn.Argument"), ColumnInfo.COLUMN_TYPE_TEXT, false, true ), // Argument name
            new ColumnInfo( BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ArgumentsColumn.Value"), ColumnInfo.COLUMN_TYPE_TEXT, false, false), // Actual value
          };
              
        int nrArguments = configuration.getArguments() !=null ? configuration.getArguments().size() : 0; 
        wArguments = new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, cArguments, nrArguments, true, null, props);
        FormData fdArguments = new FormData();
        fdArguments.left   = new FormAttachment(0, 0);
        fdArguments.right  = new FormAttachment(50, -margin);
        fdArguments.top    = new FormAttachment(wlArguments, margin);
        fdArguments.bottom = new FormAttachment(wOK, -margin*2);
        wArguments.setLayoutData(fdArguments);
        
        
        // Named parameters
        wlParams = new Label(shell, SWT.LEFT);
        props.setLook(wlParams);
        wlParams.setText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.Params.Label")); //$NON-NLS-1$
        wlParams.setToolTipText(BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.Params.Tooltip")); //$NON-NLS-1$ //$NON-NLS-2$
        FormData fdlParams = new FormData();
        fdlParams.left   = new FormAttachment(0, 0);
        fdlParams.right  = new FormAttachment(50, -margin);
        fdlParams.top    = new FormAttachment(gDetails, margin*2);
        wlParams.setLayoutData(fdlParams);

        ColumnInfo[] cParams = {
            new ColumnInfo( BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ParamsColumn.Argument"), ColumnInfo.COLUMN_TYPE_TEXT, false, false ), //Parameter name
            new ColumnInfo( BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ParamsColumn.Value"), ColumnInfo.COLUMN_TYPE_TEXT, false, false), //value
            new ColumnInfo( BaseMessages.getString(PKG, "TransExecutionConfigurationDialog.ParamsColumn.Default"), ColumnInfo.COLUMN_TYPE_TEXT, false, false), //Default Value
          };
              
        String[] namedParams = transMeta.listParameters();
        int nrParams = namedParams.length; 
        wParams = new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, cParams, nrParams, true, null, props);
        FormData fdParams = new FormData();
        fdParams.left   = new FormAttachment(0, 0);
        fdParams.right  = new FormAttachment(50, -margin);
        fdParams.top    = new FormAttachment(wlParams, margin);        
        fdParams.bottom = new FormAttachment(wlArguments, -margin * 2);
        wParams.setLayoutData(fdParams);                
        
        getData();
        
        BaseStepDialog.setSize(shell);
        
        // Set the focus on the OK button
        wOK.setFocus();
        
        shell.open();
        while (!shell.isDisposed())
        {
            if (!display.readAndDispatch())  {
            	display.sleep();
            }
        }
        
        return retval;
    }

    private void getParamsData()
    {
        wParams.clearAll(false);
        List<String> paramNames = new ArrayList<String>( configuration.getParams().keySet() );
        Collections.sort(paramNames);
        
        for (int i=0;i<paramNames.size();i++)
        {
        	String paramName = paramNames.get(i);
        	String paramValue = configuration.getParams().get(paramName);
        	String defaultValue;
			try {
				defaultValue = transMeta.getParameterDefault(paramName);
			} catch (UnknownParamException e) {
				defaultValue = "";
			}
        	
            TableItem tableItem = new TableItem(wParams.table, SWT.NONE);
            tableItem.setText(1, paramName);
            tableItem.setText(2, Const.NVL(paramValue, ""));
            tableItem.setText(3, Const.NVL(defaultValue, ""));
        }
        wParams.removeEmptyRows();
        wParams.setRowNums();
        wParams.optWidth(true);
    }    
    
    private void getVariablesData()
    {
        wVariables.clearAll(false);
        List<String> variableNames = new ArrayList<String>( configuration.getVariables().keySet() );
        Collections.sort(variableNames);
        
        for (int i=0;i<variableNames.size();i++)
        {
        	String variableName = variableNames.get(i);
        	String variableValue = configuration.getVariables().get(variableName);
        	
        	if (Const.indexOfString(variableName, transMeta.listParameters())<0) {
        	
            TableItem tableItem = new TableItem(wVariables.table, SWT.NONE);
            tableItem.setText(1, variableName);
            tableItem.setText(2, Const.NVL(variableValue, ""));
        	}
        }
        wVariables.removeEmptyRows();
        wVariables.setRowNums();
        wVariables.optWidth(true);
    }

    private void getArgumentsData()
    {
        wArguments.clearAll(false);
        
        List<String> argumentNames = new ArrayList<String>( configuration.getArguments().keySet() );
        Collections.sort(argumentNames);
        
        for (int i=0;i<argumentNames.size();i++)
        {
        	String argumentName = argumentNames.get(i);
        	String argumentValue = configuration.getArguments().get(argumentName);
        	
            TableItem tableItem = new TableItem(wArguments.table, SWT.NONE);
            tableItem.setText(1, Const.NVL(argumentName, ""));
            tableItem.setText(2, Const.NVL(argumentValue, ""));
        }
        wArguments.removeEmptyRows();
        wArguments.setRowNums();
        wArguments.optWidth(true);
    }

    private void cancel()
    {
        dispose();
    }
    
    private void dispose()
    {
        props.setScreen(new WindowProperty(shell));
        shell.dispose();
    }

    private void ok()
    {
    	if (Const.isOSX())
    	{
    		// OSX bug workaround.
    		//
    		wVariables.applyOSXChanges();
    		wParams.applyOSXChanges();
    		wArguments.applyOSXChanges();
    	}
        getInfo();
        retval=true;
        dispose();
    }
    
    public void getData()
    {
        wExecLocal.setSelection(configuration.isExecutingLocally());
        wExecRemote.setSelection(configuration.isExecutingRemotely());
        wExecCluster.setSelection(configuration.isExecutingClustered());
        wSafeMode.setSelection(configuration.isSafeModeEnabled());
        wClearLog.setSelection(configuration.isClearingLog());
        wPrepareExecution.setSelection(configuration.isClusterPreparing());
        wPostTransformation.setSelection(configuration.isClusterPosting());
        wStartExecution.setSelection(configuration.isClusterStarting());
        wShowTransformations.setSelection(configuration.isClusterShowingTransformation());
        wRemoteHost.setText( configuration.getRemoteServer()==null ? "" : configuration.getRemoteServer().toString() );
        wPassExport.setSelection(configuration.isPassingExport());
        
        wLogLevel.select( configuration.getLogLevel().getLevel() );
        if (configuration.getReplayDate()!=null) wReplayDate.setText(simpleDateFormat.format(configuration.getReplayDate()));
        getParamsData();
        getArgumentsData();
        getVariablesData();
        
        enableFields();
    }
    
    public void getInfo()
    {
        try
        {
            if (!Const.isEmpty(wReplayDate.getText()))
            {
                configuration.setReplayDate(simpleDateFormat.parse(wReplayDate.getText()));
            }
            else
            {
                configuration.setReplayDate(null);
            }
            configuration.setExecutingLocally(wExecLocal.getSelection());
            configuration.setExecutingRemotely(wExecRemote.getSelection());
            configuration.setExecutingClustered(wExecCluster.getSelection());
            
            // Local data
            // --> preview handled in debug transformation meta dialog
            
            // Remote data
            if (wExecRemote.getSelection())
            {
                String serverName = wRemoteHost.getText();
                configuration.setRemoteServer(transMeta.findSlaveServer(serverName));
            }
            configuration.setPassingExport(wPassExport.getSelection());

            // Clustering data
            configuration.setClusterPosting(wPostTransformation.getSelection());
            configuration.setClusterPreparing(wPrepareExecution.getSelection());
            configuration.setClusterStarting(wStartExecution.getSelection());
            configuration.setClusterShowingTransformation(wShowTransformations.getSelection());
            
            configuration.setSafeModeEnabled(wSafeMode.getSelection() );
            configuration.setClearingLog(wClearLog.getSelection());
            configuration.setLogLevel( LogLevel.values()[wLogLevel.getSelectionIndex()] );
            
            // The lower part of the dialog...
            getInfoParameters();
            getInfoVariables();
            getInfoArguments();
        }
        catch(Exception e)
        {
            new ErrorDialog(shell, "Error in settings", "There is an error in the dialog settings", e);
        }
    }

    /**
     * Get the parameters from the dialog.
     */
    private void getInfoParameters()
    {
        Map<String,String> map = new HashMap<String, String>();
    	int nrNonEmptyVariables = wParams.nrNonEmpty(); 
        for (int i=0;i<nrNonEmptyVariables;i++)
        {
            TableItem tableItem = wParams.getNonEmpty(i);
            String paramName = tableItem.getText(1);
            String paramValue = tableItem.getText(2);
            String defaultValue = tableItem.getText(3);
            
            if ( Const.isEmpty(paramValue) )  {
            	paramValue = Const.NVL(defaultValue, "");
            }
            
            map.put(paramName, paramValue);
        }
        configuration.setParams(map);
    }
        
    private void getInfoVariables()
    {
        Map<String,String> map = new HashMap<String, String>();
    	int nrNonEmptyVariables = wVariables.nrNonEmpty(); 
        for (int i=0;i<nrNonEmptyVariables;i++)
        {
            TableItem tableItem = wVariables.getNonEmpty(i);
            String varName = tableItem.getText(1);
            String varValue = tableItem.getText(2);
            
            if (!Const.isEmpty(varName))
            {
                map.put(varName, varValue);
            }
        }
        configuration.setVariables(map);
    }
    
    private void getInfoArguments()
    {
    	Map<String,String> map = new HashMap<String, String>();
    	int nrNonEmptyArguments = wArguments.nrNonEmpty(); 
    	for (int i=0;i<nrNonEmptyArguments;i++)
        {
            TableItem tableItem = wArguments.getNonEmpty(i);
            String varName = tableItem.getText(1);
            String varValue = tableItem.getText(2);
            
            if (!Const.isEmpty(varName))
            {
                map.put(varName, varValue);
            }
        }
        configuration.setArguments(map);
    }
    
    private void enableFields()
    {
        // boolean enableLocal = wExecLocal.getSelection();
        boolean enableRemote = wExecRemote.getSelection();
        boolean enableCluster = wExecCluster.getSelection();
                
        // wlReplayDate.setEnabled(enableLocal);
        // wReplayDate.setEnabled(enableLocal);
        // wlArguments.setEnabled(enableLocal);
        // wArguments.setEnabled(enableLocal);
        // wArguments.table.setEnabled(enableLocal);
        // wlVariables.setEnabled(enableLocal);
        // wVariables.setEnabled(enableLocal);
        // wVariables.table.setEnabled(enableLocal);
        // wSafeMode.setEnabled(enableLocal);
        
        wRemoteHost.setEnabled(enableRemote);
        wlRemoteHost.setEnabled(enableRemote);
        wPassExport.setEnabled(enableRemote);
        
        wPostTransformation.setEnabled(enableCluster);
        wPrepareExecution.setEnabled(enableCluster);
        wStartExecution.setEnabled(enableCluster);
        wShowTransformations.setEnabled(enableCluster);
    }

    /**
     * @return the configuration
     */
    public TransExecutionConfiguration getConfiguration()
    {
        return configuration;
    }

    /**
     * @param configuration the configuration to set
     */
    public void setConfiguration(TransExecutionConfiguration configuration)
    {
        this.configuration = configuration;
    }
}