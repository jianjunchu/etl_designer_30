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

package org.pentaho.di.ui.repository;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.repository.dialog.RepositoryDialogInterface;
import org.pentaho.di.ui.repository.dialog.RepositoryDialogInterface.MODE;
import org.pentaho.di.ui.repository.model.RepositoriesModel;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.components.XulConfirmBox;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.util.XulDialogCallback;

import com.kingbase.ktrl.util.Constants;

public class RepositoriesHelper {
  private static Class<?> PKG = RepositoriesHelper.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private Shell shell;
  private PropsUI props;
  private RepositoriesMeta input;
  private Repository repository;
  private String prefRepositoryName;
  private RepositoriesModel model;
  private Document document;
  private LogChannel log;
  private String initMessages;
 
  
  public RepositoriesHelper(RepositoriesModel model, Document document, Shell shell) {
    this.props = PropsUI.getInstance();
    this.input = new RepositoriesMeta();
    this.repository = null;
    this.model = model;
    this.document = document;
    this.shell = shell;
    log = new LogChannel("RepositoriesHelper"); //$NON-NLS-1$
    initMessages=testConnection();
    if("OK".equals(initMessages)){
        try {
            try {
          	  /**
          	   * bug:20697  78-80 cli
          	   */
          	//String url="http://localhost:8080/etl_platform/usermanager?action=login&user_name=cli123&password=cli123";
       //   	String url=model.getRepositoryUrl()+"/usermanager?action=login&user_name="+model.getUsername()+"&password="+model.getPassword();  
              this.input.readData();
              if(input.getErrorMessage() != null && input.getErrorMessage().length() >0 ) {
                throw new KettleException(input.getErrorMessage());
              }
            } catch (KettleException e) {
              Spoon.getInstance().hideSplash();
              log.logDetailed(BaseMessages.getString(PKG, "RepositoryLogin.ErrorReadingRepositoryDefinitions", e.getLocalizedMessage()));//$NON-NLS-1$
              new ErrorDialog(shell, BaseMessages.getString(PKG, "Dialog.Error"), BaseMessages.getString(PKG, "RepositoryLogin.ErrorReadingRepositoryDefinitions", e.getLocalizedMessage()), e); //$NON-NLS-1$ //$NON-NLS-2$
            }      
            List<RepositoryMeta> repositoryList = new ArrayList<RepositoryMeta>();
            for(int i=0; i<this.input.nrRepositories();i++) {
              repositoryList.add(this.input.getRepository(i));
            }
            if(repositoryList.size()>0)
            	prefRepositoryName=repositoryList.get(0).getName();
            model.setAvailableRepositories(repositoryList);
          } catch(Exception e) {
            log.logDetailed(BaseMessages.getString(PKG, "RepositoryLogin.ErrorReadingRepositoryDefinitions"));//$NON-NLS-1$
            new ErrorDialog(shell, BaseMessages.getString(PKG, "Dialog.Error"), BaseMessages.getString(PKG, "RepositoryLogin.ErrorReadingRepositoryDefinitions"), e); //$NON-NLS-1$ //$NON-NLS-2$
          }    	
    }

  }
  public void newRepository() {
	PluginRegistry registry = PluginRegistry.getInstance();
	Class<? extends PluginTypeInterface> pluginType = RepositoryPluginType.class;
	List<PluginInterface> plugins = registry.getPlugins(pluginType);
	
	String[] names = new String[plugins.size()];
    for (int i = 0; i < names.length; i++) {
      PluginInterface plugin = plugins.get(i);
      names[i] = plugin.getName() + " : " + plugin.getDescription(); //$NON-NLS-1$
    }

    // TODO: make this a bit fancier!
    EnterSelectionDialog selectRepositoryType = new EnterSelectionDialog(this.shell, names,
        BaseMessages.getString(PKG, "RepositoryLogin.SelectRepositoryType"), BaseMessages.getString(PKG, "RepositoryLogin.SelectRepositoryTypeCreate")); //$NON-NLS-1$//$NON-NLS-2$
    String choice = selectRepositoryType.open();
    if (choice != null) {
      int index = selectRepositoryType.getSelectionNr();
      PluginInterface plugin = plugins.get(index);
      String id = plugin.getIds()[0];

      try {
        // With this ID we can create a new Repository object...
        //
        RepositoryMeta repositoryMeta = PluginRegistry.getInstance().loadClass(RepositoryPluginType.class, id, RepositoryMeta.class);
        RepositoryDialogInterface dialog = getRepositoryDialog(plugin, repositoryMeta, input, this.shell);
        RepositoryMeta meta = dialog.open(MODE.ADD);
        if (meta != null) {
          // If the repository meta is not null and the repository name does not exist in the repositories list. 
          // If it does then display a error to the user
          if(meta.getName() != null) {
            input.addRepository(meta);
            fillRepositories();
            model.setSelectedRepository(meta);
            writeData();            
          }
        }
      } catch (Exception e) {
        log.logDetailed(BaseMessages.getString(PKG, "RepositoryLogin.ErrorCreatingRepository", e.getLocalizedMessage()));//$NON-NLS-1$
        new ErrorDialog(shell, BaseMessages.getString(PKG, "Dialog.Error"), BaseMessages.getString(PKG, "RepositoryLogin.ErrorCreatingRepository", e.getLocalizedMessage()), e); //$NON-NLS-1$ //$NON-NLS-2$
      }
    }
  }

  public void editRepository() {
    try {
        PluginInterface plugin = null; 
        RepositoryMeta ri = input.searchRepository(model.getSelectedRepository().getName());
        if (ri != null) {
          plugin = PluginRegistry.getInstance().getPlugin(RepositoryPluginType.class, ri.getId());
          if (plugin == null) {
            throw new KettleException(BaseMessages.getString(PKG, "RepositoryLogin.ErrorFindingPlugin", ri.getId())); //$NON-NLS-1$
          }
        }
          RepositoryDialogInterface dd = getRepositoryDialog(plugin, ri, input, this.shell);
          if (dd.open(MODE.EDIT) != null) {
            fillRepositories();
            int idx = input.indexOfRepository(ri);
            model.setSelectedRepository(input.getRepository(idx));
            writeData();
          }
    } catch (Exception e) {
      log.logDetailed(BaseMessages.getString(PKG, "RepositoryLogin.ErrorEditingRepository", e.getLocalizedMessage()));//$NON-NLS-1$
      new ErrorDialog(shell, BaseMessages.getString(PKG, "Dialog.Error"), BaseMessages.getString(PKG, "RepositoryLogin.ErrorEditingRepository", e.getLocalizedMessage()), e); //$NON-NLS-1$ //$NON-NLS-2$
    }
  }

  public void deleteRepository() {
    try {
        XulConfirmBox confirmBox = (XulConfirmBox) document.createElement("confirmbox");//$NON-NLS-1$
        final RepositoryMeta repositoryMeta = input.searchRepository(model.getSelectedRepository().getName());
        if (repositoryMeta != null) {
          confirmBox.setTitle(BaseMessages.getString(PKG, "RepositoryLogin.ConfirmDeleteRepositoryDialog.Title"));//$NON-NLS-1$
          confirmBox.setMessage(BaseMessages.getString(PKG, "RepositoryLogin.ConfirmDeleteRepositoryDialog.Message"));//$NON-NLS-1$
          confirmBox.setAcceptLabel(BaseMessages.getString(PKG, "Dialog.Ok"));//$NON-NLS-1$
          confirmBox.setCancelLabel(BaseMessages.getString(PKG, "Dialog.Cancel"));//$NON-NLS-1$
          confirmBox.addDialogCallback(new XulDialogCallback<Object>() {
    
            public void onClose(XulComponent sender, Status returnCode, Object retVal) {
              if (returnCode == Status.ACCEPT) {
                int idx = input.indexOfRepository(repositoryMeta);
                input.removeRepository(idx);
                fillRepositories();
                writeData();
              }
            }
    
            public void onError(XulComponent sender, Throwable t) {
              log.logDetailed(BaseMessages.getString(PKG, "RepositoryLogin.UnableToDeleteRepository", t.getLocalizedMessage()));//$NON-NLS-1$
              new ErrorDialog(shell, BaseMessages.getString(PKG, "Dialog.Error"), BaseMessages.getString(PKG, "RepositoryLogin.UnableToDeleteRepository", t.getLocalizedMessage()), t);//$NON-NLS-1$ //$NON-NLS-2$
            }
          });
          confirmBox.open();
        }
    } catch (Exception e) {
      log.logDetailed(BaseMessages.getString(PKG, "RepositoryLogin.UnableToDeleteRepository", e.getLocalizedMessage()));//$NON-NLS-1$
      new ErrorDialog(shell, BaseMessages.getString(PKG, "Dialog.Error"), BaseMessages.getString(PKG, "RepositoryLogin.UnableToDeleteRepository", e.getLocalizedMessage()), e); //$NON-NLS-1$ //$NON-NLS-2$
    }      
  }

  protected RepositoryDialogInterface getRepositoryDialog(PluginInterface plugin, RepositoryMeta repositoryMeta, RepositoriesMeta input2, Shell shell) throws Exception {
	String className = repositoryMeta.getDialogClassName();
    Class<? extends RepositoryDialogInterface> dialogClass = PluginRegistry.getInstance().getClass(plugin, className);
    Constructor<?> constructor = dialogClass.getConstructor(Shell.class, Integer.TYPE, RepositoryMeta.class, RepositoriesMeta.class);
    return (RepositoryDialogInterface) constructor.newInstance(new Object[] { shell, Integer.valueOf(SWT.NONE), repositoryMeta, input, });
  }
  

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getMetaData() {
    fillRepositories();

    String repname = props.getLastRepository();
    if (repname != null) {
      model.setSelectedRepositoryUsingName(repname);
      String username = props.getLastRepositoryLogin();
      if (username != null) {
        model.setUsername(username);
      }
    }

    // Do we have a preferred repository name to select
    if (prefRepositoryName != null) {
      model.setSelectedRepositoryUsingName(prefRepositoryName);
    }

    model.setShowDialogAtStartup(props.showRepositoriesDialogAtStartup());    
  }
  
  /**
   * add by cli 239-241  the selected repository is default to the first one
   */
  public void setPreferedReopsitory(){
	  model.setSelectedRepositoryUsingName(prefRepositoryName);
  }

  
  public void fillRepositories() {
    model.getAvailableRepositories().clear();
    if(input.nrRepositories() == 0) {
      model.addToAvailableRepositories(null);
    } else {
      for (int i = 0; i < input.nrRepositories(); i++) {
        model.addToAvailableRepositories(input.getRepository(i));
      }
    }
  }
  
  public Repository getConnectedRepository() {
    return repository;
  }

  public void setPreferredRepositoryName(String repname) {
    prefRepositoryName = repname;
  }
  
  public void loginToRepository() throws KettleException, KettleSecurityException{
    if(model != null && model.getSelectedRepository() != null) {
      RepositoryMeta repositoryMeta = input.getRepository(model.getRepositoryIndex(model.getSelectedRepository()));
      repository = PluginRegistry.getInstance().loadClass(RepositoryPluginType.class, repositoryMeta.getId(), Repository.class);
      repository.init(repositoryMeta);
	  /**
	   * bug:20697  259-260 cli
	   */
    //  repository.connect(model.getUsername(), model.getPassword());
      repository.connect("admin", "admin");
      props.setLastRepository(repositoryMeta.getName());
      props.setLastRepositoryLogin(model.getUsername());
    } else {
      log.logDetailed(BaseMessages.getString(PKG, "RepositoryLogin.ErrorLoginToRepository"));//$NON-NLS-1$
      throw new KettleException(BaseMessages.getString(PKG, "RepositoryLogin.ErrorLoginToRepository"));//$NON-NLS-1$
    }
  }
  
  public void updateShowDialogOnStartup(boolean value) {
    props.setRepositoriesDialogAtStartupShown(value);
  }
  
  private void writeData() {
    try {
      input.writeData();
    } catch (Exception e) {
      log.logDetailed(BaseMessages.getString(PKG, "RepositoryLogin.ErrorSavingRepositoryDefinition", e.getLocalizedMessage())); //$NON-NLS-1$
      new ErrorDialog(shell, BaseMessages.getString(PKG, "Dialog.Error"), BaseMessages.getString(PKG, "RepositoryLogin.ErrorSavingRepositoryDefinition", e.getLocalizedMessage()), e); //$NON-NLS-1$ //$NON-NLS-2$
    } 
  }
  /**
   * for bug 20697 
   * checek the URL ,usrename and password
   * @return
   */
  public String testConnection(){
	  String url=model.getRepositoryUrl()+"/login?action=login&user_name="+model.getUsername()+"&password="+model.getPassword();
	  //xnren start set server_url
	  try{
		  Properties p = new Properties();
	      String propFile = Const.getKettleDirectory()+"/"+Const.KETTLE_PROPERTIES;
	      p.load(new FileInputStream(propFile));
	      Writer w=new FileWriter(propFile);
	      p.setProperty("SERVER_URL", model.getRepositoryUrl());
	      p.store(w, "SERVER_URL");
	  }catch (Exception e){
		  e.printStackTrace();
	  }
	  //xnren end
	  return input.testConnection(url);
  }
  /**
   * for bug 20697
   * return the connection test result
   * @return
   */
  public String getInitMessages(){
	  return this.initMessages;
  }
  public int getRepositoryCount(){
	  return this.input.nrRepositories();
  }

}
