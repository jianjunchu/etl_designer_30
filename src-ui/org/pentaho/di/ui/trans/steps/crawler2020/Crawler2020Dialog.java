/*
 * Copyright (c) 2007 Pentaho Corporation.  All rights reserved. 
 * This software was developed by Pentaho Corporation and is provided under the terms 
 * of the GNU Lesser General Public License, Version 2.1. You may not use 
 * this file except in compliance with the license. If you need a copy of the license, 
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. The Original Code is Pentaho 
 * Data Integration.  The Initial Developer is Samatarn.
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS" 
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to 
 * the license for the specific language governing your rights and limitations.
*/

/*
 * Created on 18-mei-2003
 *
 */

package org.pentaho.di.ui.trans.steps.crawler2020;

import java.io.FileNotFoundException;
import java.util.Date;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.trans.steps.crawler2020.Crawler2020Meta;
import org.pentaho.di.i18n.BaseMessages;

public class Crawler2020Dialog extends BaseStepDialog implements StepDialogInterface
{
	private CTabFolder wTabFolder;

	private FormData fdTabFolder;

	private CTabItem wFileTab, wFilterTab;
	
	private Composite wFileComp;
	
	private FormData fdFileComp;

	
	private Label wlFilename;
	private Button wbdFilename; // Delete
	private Button wbaFilename; // Add or change
	private TextVar wFilename;
	private FormData fdlFilename, fdbFilename, fdbdFilename, fdbeFilename, fdbaFilename, fdFilename;

	private Label wlFilenameList;
	private TableView wFilenameList;
	private FormData fdlFilenameList, fdFilenameList;

	private Crawler2020Meta input;

	private int middle, margin;

	private ModifyListener lsMod;

	private Group wOriginFiles;
	
	private Label        wlLimit;
	private Text         wLimit;
	private FormData     fdlLimit, fdLimit;
	
	private Label        wlInclRownum;
	private Button       wInclRownum;
	private FormData     fdlInclRownum, fdRownum;

	private Label        wlInclRownumField;
	private TextVar      wInclRownumField;
	private FormData     fdlInclRownumField, fdInclRownumField;
	
	private boolean 	getpreviousFields=false;

	private Label wlFieldnameList;
	private TableView wFieldnameList;
	private FormData fdlFieldnameList, fdFieldnameList;

	private Label wlHint;//rate 
	private Label wHint;
	private FormData fdlHint, fdHint;
	
	public Crawler2020Dialog(Shell parent, Object in, TransMeta transMeta, String sname)
	{
		super(parent, (BaseStepMeta) in, transMeta, sname);
		input = (Crawler2020Meta) in;
	}

	public String open()
	{
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
		props.setLook(shell);
		setShellImage(shell, input);

		lsMod = new ModifyListener()
		{
			public void modifyText(ModifyEvent e)
			{
				input.setChanged();
			}
		};
		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString("GanjiDialog.DialogTitle") + input.getVersionName());

		middle = props.getMiddlePct();
		margin = Const.MARGIN;

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString("System.Label.StepName"));
		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.top = new FormAttachment(0, margin);
		fdlStepname.right = new FormAttachment(middle, -margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		wTabFolder = new CTabFolder(shell, SWT.BORDER);
		props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

		// ////////////////////////
		// START OF FILE TAB ///
		// ////////////////////////
		wFileTab = new CTabItem(wTabFolder, SWT.NONE);
		wFileTab.setText(BaseMessages.getString("GanjiDialog.FileTab.TabTitle"));

		wFileComp = new Composite(wTabFolder, SWT.NONE);
		props.setLook(wFileComp);

		FormLayout fileLayout = new FormLayout();
		fileLayout.marginWidth = 3;
		fileLayout.marginHeight = 3;
		wFileComp.setLayout(fileLayout);
		
	


		// Filename line
		wlFilename = new Label(wFileComp, SWT.RIGHT);
		wlFilename.setText(BaseMessages.getString("GanjiDialog.Filename.Label"));
		props.setLook(wlFilename);
		fdlFilename = new FormData();
		fdlFilename.left = new FormAttachment(0, 0);
		fdlFilename.top = new FormAttachment(wOriginFiles, margin);
		fdlFilename.right = new FormAttachment(middle, -margin);
		wlFilename.setLayoutData(fdlFilename);



		wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
		props.setLook(wbaFilename);
		wbaFilename.setText(BaseMessages.getString("GanjiDialog.FilenameAdd.Button"));
		wbaFilename.setToolTipText(BaseMessages.getString("GanjiDialog.FilenameAdd.Tooltip"));
		fdbaFilename = new FormData();
		fdbaFilename.right = new FormAttachment(100, -margin);
		fdbaFilename.top = new FormAttachment(wOriginFiles, margin);
		wbaFilename.setLayoutData(fdbaFilename);

		wFilename = new TextVar(transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		props.setLook(wFilename);
		wFilename.addModifyListener(lsMod);
		fdFilename = new FormData();
		fdFilename.left = new FormAttachment(middle, 0);
		fdFilename.right = new FormAttachment(wbaFilename, -margin);
		fdFilename.top = new FormAttachment(wOriginFiles, margin);
		wFilename.setLayoutData(fdFilename);

		
		///////////////////////
		wlFilenameList = new Label(wFileComp, SWT.RIGHT); 
		wlFilenameList.setText(BaseMessages.getString("GanjiDialog.FilenameList.Label"));
		props.setLook(wlFilenameList);
		fdlFilenameList = new FormData();
		fdlFilenameList.left = new FormAttachment(0, 0);
		fdlFilenameList.top = new FormAttachment(wFilename, margin);
		fdlFilenameList.right = new FormAttachment(middle, -margin);
		wlFilenameList.setLayoutData(fdlFilenameList);



//		wbaDirname = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
//		props.setLook(wbaDirname);
//		wbaDirname.setText(Messages.getString("GanjiDialog.FilenameAdd.Button"));
//		wbaDirname.setToolTipText(Messages.getString("GanjiDialog.FilenameAdd.Tooltip"));
//		fdbaDirname = new FormData();
//		fdbaDirname.right = new FormAttachment(wbbDirname, -margin);
//		fdbaDirname.top = new FormAttachment(wFilename, margin);
//		wbaDirname.setLayoutData(fdbaDirname);
//
//		wDirname = new TextVar(transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
//		props.setLook(wDirname);
//		wDirname.addModifyListener(lsMod);
//		fdDirname = new FormData();
//		fdDirname.left = new FormAttachment(middle, 0);
//		fdDirname.right = new FormAttachment(wbaDirname, -margin);
//		fdDirname.top = new FormAttachment(wFilename, margin);
//		wDirname.setLayoutData(fdDirname);



		// Buttons to the right of the screen...
		wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
		props.setLook(wbdFilename);
		wbdFilename.setText(BaseMessages.getString("GanjiDialog.FilenameDelete.Button"));
		wbdFilename.setToolTipText(BaseMessages.getString("GanjiDialog.FilenameDelete.Tooltip"));
		fdbdFilename = new FormData();
		fdbdFilename.right = new FormAttachment(100, 0);
		fdbdFilename.top = new FormAttachment(wFilename, 40);
		wbdFilename.setLayoutData(fdbdFilename);


		ColumnInfo[] colinfo = new ColumnInfo[] {
				new ColumnInfo("�б�ҳ��ַ",
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo("��ȡ��ҳ��",
								ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo("�Ƿ��ȡ",
						ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { BaseMessages.getString("System.Combo.Yes"), BaseMessages.getString("System.Combo.No")})};

        colinfo[0].setUsingVariables(true);
        
        colinfo[0].setToolTip("������һ���б�ҳ��ĵ�ַ���� http://bj.ganji.com/fang1");
        colinfo[1].setToolTip("һ��Ҫ��ȡ�����ļ�ҳ");
		colinfo[2].setToolTip("���û���ͣ�������ַ�ĳ�ȡ");

		wFilenameList = new TableView(transMeta, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo,
				colinfo.length, lsMod, props);
		props.setLook(wFilenameList);
		fdFilenameList = new FormData();
		fdFilenameList.left = new FormAttachment(middle, 0);
		fdFilenameList.right = new FormAttachment(wbdFilename, -margin);
		fdFilenameList.top = new FormAttachment(wFilename, margin);
		fdFilenameList.bottom = new FormAttachment(30, margin);
		wFilenameList.setLayoutData(fdFilenameList);

		fdFileComp = new FormData();
		fdFileComp.left = new FormAttachment(0, 0);
		fdFileComp.top = new FormAttachment(0, 0);
		fdFileComp.right = new FormAttachment(100, 0);
		fdFileComp.bottom = new FormAttachment(100, 0);
		wFileComp.setLayoutData(fdFileComp);

		wFileComp.layout();
		wFileTab.setControl(wFileComp);

		// ///////////////////////////////////////////////////////////
		// / END OF FILE TAB
		// ///////////////////////////////////////////////////////////

		fdTabFolder = new FormData();
		fdTabFolder.left = new FormAttachment(0, 0);
		fdTabFolder.top = new FormAttachment(wStepname, margin);
		fdTabFolder.right = new FormAttachment(100, 0);
		fdTabFolder.bottom = new FormAttachment(100, -50);
		wTabFolder.setLayoutData(fdTabFolder);
		
		
//		wlInclRownum=new Label(wFileComp, SWT.RIGHT);
//		wlInclRownum.setText(Messages.getString("GanjiDialog.InclRownum.Label"));
// 		props.setLook(wlInclRownum);
//		fdlInclRownum=new FormData();
//		fdlInclRownum.left = new FormAttachment(0, 0);
//		fdlInclRownum.top  = new FormAttachment(wFilenameList, 2*margin);
//		fdlInclRownum.right= new FormAttachment(middle, -margin);
//		wlInclRownum.setLayoutData(fdlInclRownum);
//		wInclRownum=new Button(wFileComp, SWT.CHECK );
// 		props.setLook(wInclRownum);
//		wInclRownum.setToolTipText(Messages.getString("GanjiDialog.InclRownum.Tooltip"));
//		fdRownum=new FormData();
//		fdRownum.left = new FormAttachment(middle, 0);
//		fdRownum.top  = new FormAttachment(wFilenameList, 2*margin);
//		wInclRownum.setLayoutData(fdRownum);
//
//		wlInclRownumField=new Label(wFileComp, SWT.RIGHT);
//		wlInclRownumField.setText(Messages.getString("GanjiDialog.InclRownumField.Label"));
// 		props.setLook(wlInclRownumField);
//		fdlInclRownumField=new FormData();
//		fdlInclRownumField.left = new FormAttachment(wInclRownum, margin);
//		fdlInclRownumField.top  = new FormAttachment(wFilenameList, 2*margin);
//		wlInclRownumField.setLayoutData(fdlInclRownumField);
//		wInclRownumField=new TextVar(transMeta,wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
// 		props.setLook(wInclRownumField);
//		wInclRownumField.addModifyListener(lsMod);
//		fdInclRownumField=new FormData();
//		fdInclRownumField.left = new FormAttachment(wlInclRownumField, margin);
//		fdInclRownumField.top  = new FormAttachment(wFilenameList, 2*margin);
//		fdInclRownumField.right= new FormAttachment(100, 0);
//		wInclRownumField.setLayoutData(fdInclRownumField);
		
		// ///////////////////////////////////////////////////////////
		// / END OF DESTINATION ADDRESS  GROUP
		// ///////////////////////////////////////////////////////////
		
		wlFieldnameList = new Label(wFileComp, SWT.RIGHT); 
		wlFieldnameList.setText(BaseMessages.getString("GanjiDialog.FieldnameList.Label"));
		props.setLook(wlFieldnameList);
		fdlFieldnameList = new FormData();
		fdlFieldnameList.left = new FormAttachment(0, 0);
		fdlFieldnameList.top = new FormAttachment(wFilenameList, margin);
		fdlFieldnameList.right = new FormAttachment(middle, -margin);
		wlFieldnameList.setLayoutData(fdlFieldnameList);


		ColumnInfo[] fieldinfo = new ColumnInfo[] {
				new ColumnInfo("ҳ�������Ϣ������",
						ColumnInfo.COLUMN_TYPE_TEXT, false),
				new ColumnInfo("Ҫ���浽���ֶ���",
								ColumnInfo.COLUMN_TYPE_TEXT, false)};

		fieldinfo[0].setUsingVariables(true);
        
		fieldinfo[0].setToolTip("����ҳ����Ҫ��ȡ��Щ��Ϣ���������Щ��Ŀ��ҳ��������ơ��� ����ʱ��: ");
		fieldinfo[1].setToolTip("Ҫ���浽���ֶ���");
        
		wFieldnameList = new TableView(transMeta, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, fieldinfo,
				fieldinfo.length, lsMod, props);
		props.setLook(wFieldnameList);
		fdFieldnameList = new FormData();
		fdFieldnameList.left = new FormAttachment(middle, 0);
		fdFieldnameList.top = new FormAttachment(wFilenameList, margin);
		fdFieldnameList.bottom = new FormAttachment(70, margin);
		wFieldnameList.setLayoutData(fdFieldnameList);
		
		wlLimit=new Label(wFileComp, SWT.RIGHT);
		wlLimit.setText(BaseMessages.getString("GanjiDialog.Limit.Label"));
 		props.setLook(wlLimit);
		fdlLimit=new FormData();
		fdlLimit.left = new FormAttachment(0, 0);
		fdlLimit.top  = new FormAttachment(wFieldnameList, 2*margin);
		fdlLimit.right= new FormAttachment(middle, -margin);
		wlLimit.setLayoutData(fdlLimit);
		wLimit=new Text(wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wLimit);
		wLimit.addModifyListener(lsMod);
		fdLimit=new FormData();
		fdLimit.left = new FormAttachment(middle, 0);
		fdLimit.top  = new FormAttachment(wFieldnameList, 2*margin);
		fdLimit.right= new FormAttachment(100, 0);
		wLimit.setLayoutData(fdLimit);
		
		
		
		wlHint=new Label(wFileComp, SWT.RIGHT);
		wlHint.setText("��ʾ��Ϣ"); //$NON-NLS-1$
 		props.setLook(wlHint);
		fdlHint=new FormData();
		fdlHint.left = new FormAttachment(0, 0);
		fdlHint.right= new FormAttachment(middle, -margin);
		fdlHint.top  = new FormAttachment(wLimit, margin);
		wlHint.setLayoutData(fdlHint);		
		wHint=new Label(wFileComp, SWT.WRAP | SWT.LEFT | SWT.BORDER);
		wHint.setText(
				"\r\n"+
				"�б�ҳ����ָ��ʾ�����б��ҳ�棬��http://bj.ganji.com/fang1/��http://bj.ganji.com/fang1/a1/"+"\r\n"+
				"����ҳ����ָ��ʾ������Ϣ��ҳ�棬��http://bj.ganji.com/fang1/tuiguang-1431243.htm���û������Զ���Ҫ�ɼ�����ҳ�������Щ��Ϣ��緿�ͣ����򣬱��۵ȡ�"+"\r\n"+
                "�ɼ���������û��Զ������Ϣ���⣬�������ĸ����õ���Ϣ���Ϣ���⣬�绰���룬����ʱ�䣬����ҳ�����ַ��"+"\r\n"+"\r\n"				
		);
 		props.setLook(wHint);
		fdHint=new FormData();
		fdHint.left = new FormAttachment(middle, 0);
		fdHint.top  = new FormAttachment(wLimit, margin);
		fdHint.right= new FormAttachment(100, 0);
		wHint.setLayoutData(fdHint);
		
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString("System.Button.OK"));

		//preview for later use, preview extracted url contents
		//wPreview = new Button(shell, SWT.PUSH);
		//wPreview.setText(Messages.getString("GanjiDialog.Preview.Button"));

		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString("System.Button.Cancel"));

		//setButtonPositions(new Button[] { wOK, wCancel , wPreview }, margin, wTabFolder);
		setButtonPositions(new Button[] { wOK, wCancel  }, margin, wTabFolder);
		// Add listeners
		lsOK = new Listener()
		{
			public void handleEvent(Event e)
			{
				ok();
			}
		};
		lsPreview = new Listener()
		{
			public void handleEvent(Event e)
			{
				preview();
			}
		};
		lsCancel = new Listener()
		{
			public void handleEvent(Event e)
			{
				cancel();
			}
		};

		wOK.addListener(SWT.Selection, lsOK);
		//wPreview.addListener(SWT.Selection, lsPreview);
		wCancel.addListener(SWT.Selection, lsCancel);

		lsDef = new SelectionAdapter()
		{
			public void widgetDefaultSelected(SelectionEvent e)
			{
				ok();
			}
		};

		wStepname.addSelectionListener(lsDef);

		// Add the url to the list of urls...
		SelectionAdapter selFile = new SelectionAdapter()
		{
			public void widgetSelected(SelectionEvent arg0)
			{
				if(wFilename.getText()==null || wFilename.getText().length()==0)
					return;
				wFilenameList.add(new String[] { wFilename.getText(), "1","��" });
				wFilename.setText("");
				wFilenameList.removeEmptyRows();
				wFilenameList.setRowNums();
				wFilenameList.optWidth(true);
			}
		};
		
//		// Add the dir and mask to the list of files...
//		SelectionAdapter selDir = new SelectionAdapter()
//		{
//			public void widgetSelected(SelectionEvent arg0)
//			{
//				wFilename.setText("");
//				wFilenameList.add(new String[] { wFilename.getText(),"��" });
//				wFilenameList.removeEmptyRows();
//				wFilenameList.setRowNums();
//				wFilenameList.optWidth(true);
//			}
//		};
		

		wbaFilename.addSelectionListener(selFile);
		wFilename.addSelectionListener(selFile);
		
//		wbaDirname.addSelectionListener(selDir);
//		wDirname.addSelectionListener(selDir);

		// Delete files from the list of files...
		wbdFilename.addSelectionListener(new SelectionAdapter()
		{
			public void widgetSelected(SelectionEvent arg0)
			{
				int idx[] = wFilenameList.getSelectionIndices();
				wFilenameList.remove(idx);
				wFilenameList.removeEmptyRows();
				wFilenameList.setRowNums();
			}
		});
		
		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter()
		{
			public void shellClosed(ShellEvent e)
			{
				cancel();
			}
		});

		wTabFolder.setSelection(0);

		getData(input);
		setSize();

		shell.open();
		while (!shell.isDisposed())
		{
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}
	
	/**
	 * Read the data from the TextFileInputMeta object and show it in this
	 * dialog.
	 * 
	 * @param meta
	 *            The TextFileInputMeta object to obtain the data from.
	 */
	public void getData(Crawler2020Meta meta)
	{
		final Crawler2020Meta in = meta;

		if (in.getUrls() != null)
		{
			wFilenameList.removeAll();
			for (int i = 0; i < in.getUrls().length; i++)
			{		
				wFilenameList.add(new String[] { in.getUrls()[i], in.getListPageCount()[i], in.getRequired()[i] });
			}
			
			wFilenameList.removeEmptyRows();
			wFilenameList.setRowNums();
			wFilenameList.optWidth(true);
			
			wFieldnameList.removeAll();
			for (int i = 0; i < in.getFieldNames().length; i++)
			{		
				wFieldnameList.add(new String[] { in.getKeyNames()[i], in.getFieldNames()[i]});
			}
			wFieldnameList.removeEmptyRows();
			wFieldnameList.optWidth(true);
			
			//wInclRownum.setSelection(in.isIncludeRowNumber());
			//if (in.getRowNumberField()!=null) wInclRownumField.setText(in.getRowNumberField());
			
			wLimit.setText(""+in.getRowLimit());
		}
		wStepname.selectAll();
	}

	private void cancel()
	{
		stepname = null;
		input.setChanged(changed);
		dispose();
	}

	private void ok()
	{
		if (Const.isEmpty(wStepname.getText())) return;

		getInfo(input);
		dispose();
	}

	private void getInfo(Crawler2020Meta in)
	{
		stepname = wStepname.getText(); // return value
		int nrfiles = wFilenameList.getItemCount();
		int nrfields = wFieldnameList.getItemCount();
		in.allocate(nrfiles,nrfields);

		in.setUrls(wFilenameList.getItems(0));
		in.setListPageCount(wFilenameList.getItems(1));
		in.setRequired(wFilenameList.getItems(2));
		in.setKeyNames(wFieldnameList.getItems(0));
		in.setFieldNames(wFieldnameList.getItems(1));
		
//		in.setIncludeRowNumber( wInclRownum.getSelection() );
//		in.setRowNumberField( wInclRownumField.getText() );
		in.setRowLimit( Const.toLong(wLimit.getText(), 0L) );
		if(in.getVersionName().equalsIgnoreCase(Crawler2020Meta.VERSION_FREE))
		{
			MessageBox messageBox = new MessageBox(this.getParent(), SWT.ICON_INFORMATION | SWT.OK);		        
			messageBox.setText("����");
			messageBox.setMessage("����һ����Ѱ�ĸϼ�����Ϣ�ɼ��������ֻ�ܳ�ȡ "+ in.RESTRICT_MAX_LINE+" �����ݣ��Ҳɼ����ĵ绰����ĺ���λ���Ρ����Ҫ��ȡ��ҵ�棬���� support@pentahochina.com ��ȡע���ļ���");
			int buttonID = messageBox.open();
		}
        
	}

	// Preview the data
	private void preview()
	{
		// Create the XML input step
		Crawler2020Meta oneMeta = new Crawler2020Meta();
		getInfo(oneMeta);

		TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(transMeta, oneMeta, wStepname
				.getText());

		EnterNumberDialog numberDialog = new EnterNumberDialog(shell, props.getDefaultPreviewSize(), BaseMessages.getString("GanjiDialog.PreviewSize.DialogTitle"), BaseMessages.getString("GanjiDialog.PreviewSize.DialogMessage"));
		int previewSize = numberDialog.open();
		if (previewSize > 0)
		{
			TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog(shell, previewMeta,
					new String[] { wStepname.getText() }, new int[] { previewSize });
			progressDialog.open();

			if (!progressDialog.isCancelled())
			{
				Trans trans = progressDialog.getTrans();
				String loggingText = progressDialog.getLoggingText();

				if (trans.getResult() != null && trans.getResult().getNrErrors() > 0)
				{
					EnterTextDialog etd = new EnterTextDialog(shell, BaseMessages.getString("System.Dialog.Error.Title"), BaseMessages.getString("GanjiDialog.ErrorInPreview.DialogMessage"), loggingText, true);
					etd.setReadOnly();
					etd.open();
				}
				
				PreviewRowsDialog prd = new PreviewRowsDialog(shell, transMeta, SWT.NONE, wStepname.getText(),progressDialog.getPreviewRowsMeta(wStepname.getText()),
						progressDialog.getPreviewRows(wStepname.getText()), loggingText);
				prd.open();
			}
		}
	}

	public String toString()
	{
		return this.getClass().getName();
	}
}
