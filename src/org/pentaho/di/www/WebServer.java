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

package org.pentaho.di.www;

import java.io.File;
import java.util.*;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import cn.hutool.core.util.StrUtil;
import org.eclipse.jetty.jaas.JAASLoginService;
import org.eclipse.jetty.security.*;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Credential;
import org.eclipse.jetty.util.security.Password;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.i18n.BaseMessages;



public class WebServer
{
	private static Class<?> PKG = WebServer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

    private LogChannelInterface log;
    
    public  static final int PORT = 80;

    private Server             server;
    
    private TransformationMap  transformationMap;
	private JobMap             jobMap;
	private List<SlaveServerDetection>  detections;
	private SocketRepository   socketRepository;
	
    private String hostname;
    private int port;

    private Timer slaveMonitoringTimer;

    public WebServer(LogChannelInterface log, TransformationMap transformationMap, JobMap jobMap, SocketRepository socketRepository, List<SlaveServerDetection> detections, String hostname, int port, boolean join) throws Exception
    {
    	this.log = log;
        this.transformationMap = transformationMap;
        this.jobMap = jobMap;
        this.socketRepository = socketRepository;
        this.detections = detections;
        this.hostname = hostname;
        this.port = port;

        startServer();
        
        // Start the monitoring of the registered slave servers...
        //
        startSlaveMonitoring();
        
        if (join) {
            server.join();
        }
    }

	public WebServer(LogChannelInterface log, TransformationMap transformationMap, JobMap jobMap, SocketRepository socketRepository, List<SlaveServerDetection> slaveServers, String hostname, int port) throws Exception
    {
      this(log, transformationMap, jobMap, socketRepository, slaveServers, hostname, port, true);
    }

    public Server getServer()
    {
        return server;
    }
    private String passwordFile;

    /*public void startServer() throws Exception
    {
        server = new Server();

        Constraint constraint = new Constraint();
        constraint.setName(Constraint.__BASIC_AUTH);;
        constraint.setRoles( new String[] { Constraint.ANY_ROLE } );
        constraint.setAuthenticate(true);

        ConstraintMapping constraintMapping = new ConstraintMapping();
        constraintMapping.setConstraint(constraint);
        constraintMapping.setPathSpec("/*");

        // Set up the security handler, optionally with JAAS
        //
        SecurityHandler securityHandler = new SecurityHandler();

        if(System.getProperty("loginmodulename") != null && System.getProperty("java.security.auth.login.config") != null){
            JAASUserRealm jaasRealm = new JAASUserRealm("Kettle");
            jaasRealm.setLoginModuleName(System.getProperty("loginmodulename"));
            securityHandler.setUserRealm(jaasRealm);
        } else {
            // See if there is a kettle.pwd file in the KETTLE_HOME directory:
            //
            File homePwdFile = new File(Const.getKettleCartePasswordFile());
            if (homePwdFile.exists()) {
                securityHandler.setUserRealm(new HashUserRealm("Kettle", Const.getKettleCartePasswordFile()));
            } else {
                securityHandler.setUserRealm(new HashUserRealm("Kettle", Const.getKettleLocalCartePasswordFile()));
            }
        }

        securityHandler.setConstraintMappings(new ConstraintMapping[]{constraintMapping});

        // Add all the servlets...
        //
        ContextHandlerCollection contexts = new ContextHandlerCollection();

        // Root
        //
        ServletContextHandler root = new ServletContextHandler(contexts, GetRootServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        GetRootServlet rootServlet = new GetRootServlet();
        rootServlet.setJettyMode(true);
        root.addServlet(new ServletHolder(rootServlet), "/*");

        // Carte Status
        //
        ServletContextHandler status = new ServletContextHandler(contexts, GetStatusServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        status.addServlet(new ServletHolder(new GetStatusServlet(transformationMap, jobMap)), "/*");

        // Trans status
        //
        ServletContextHandler transStatus = new ServletContextHandler(contexts, GetTransStatusServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        transStatus.addServlet(new ServletHolder(new GetTransStatusServlet(transformationMap)), "/*");

        // Prepare execution
        //
        ServletContextHandler prepareExecution = new ServletContextHandler(contexts, PrepareExecutionTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        prepareExecution.addServlet(new ServletHolder(new PrepareExecutionTransServlet(transformationMap)), "/*");

        // Start execution
        //
        ServletContextHandler startExecution = new ServletContextHandler(contexts, StartExecutionTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        startExecution.addServlet(new ServletHolder(new StartExecutionTransServlet(transformationMap)), "/*");

        // Start transformation
        //
        ServletContextHandler startTrans = new ServletContextHandler(contexts, StartTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        startTrans.addServlet(new ServletHolder(new StartTransServlet(transformationMap)), "/*");

        // Pause/Resume transformation
        //
        ServletContextHandler pauseTrans = new ServletContextHandler(contexts, PauseTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        pauseTrans.addServlet(new ServletHolder(new PauseTransServlet(transformationMap)), "/*");

        // Stop transformation
        //
        ServletContextHandler stopTrans = new ServletContextHandler(contexts, StopTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        stopTrans.addServlet(new ServletHolder(new StopTransServlet(transformationMap)), "/*");

        // Transformation cleanup
        //
        ServletContextHandler cleanupTrans = new ServletContextHandler(contexts, CleanupTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        cleanupTrans.addServlet(new ServletHolder(new CleanupTransServlet(transformationMap)), "/*");

        // Add transformation
        //
        ServletContextHandler addTrans = new ServletContextHandler(contexts, AddTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        addTrans.addServlet(new ServletHolder(new AddTransServlet(transformationMap, socketRepository)), "/*");

        // Remove Transformation
        //
        ServletContextHandler removeTrans = new ServletContextHandler(contexts, RemoveTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        removeTrans.addServlet(new ServletHolder(new RemoveTransServlet(transformationMap)), "/*");

        // Step port reservation
        //
        ServletContextHandler getPort = new ServletContextHandler(contexts, AllocateServerSocketServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        getPort.addServlet(new ServletHolder(new AllocateServerSocketServlet(transformationMap)), "/*");

        // Port listing information
        //
        ServletContextHandler listPorts = new ServletContextHandler(contexts, ListServerSocketServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        listPorts.addServlet(new ServletHolder(new ListServerSocketServlet(transformationMap)), "/*");

        // Sniff transformation step
        //
        ServletContextHandler sniffStep = new ServletContextHandler(contexts, SniffStepServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        sniffStep.addServlet(new ServletHolder(new SniffStepServlet(transformationMap)), "/*");

        // execute transformation
        //
        ServletContextHandler executeTrans = new ServletContextHandler(contexts, ExecuteTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        executeTrans .addServlet(new ServletHolder(new ExecuteTransServlet(transformationMap)), "/*");

        // add repository
        //
        ServletContextHandler addRep = new ServletContextHandler(contexts, AddRepServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        addRep .addServlet(new ServletHolder(new AddRepServlet(transformationMap)), "/*");

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        //
        // The job handlers...
        //

        // Start job
        //
        ServletContextHandler startJob = new ServletContextHandler(contexts, StartJobServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        startJob.addServlet(new ServletHolder(new StartJobServlet(jobMap)), "/*");

        // Stop transformation
        //
        ServletContextHandler stopJob = new ServletContextHandler(contexts, StopJobServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        stopJob.addServlet(new ServletHolder(new StopJobServlet(jobMap)), "/*");

        // Trans status
        //
        ServletContextHandler jobStatus = new ServletContextHandler(contexts, GetJobStatusServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        jobStatus.addServlet(new ServletHolder(new GetJobStatusServlet(jobMap)), "/*");

        // Add job
        //
        ServletContextHandler addJob= new ServletContextHandler(contexts, AddJobServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        addJob.addServlet(new ServletHolder(new AddJobServlet(jobMap, socketRepository)), "/*");

        // Remove Job
        //
        ServletContextHandler removeJob = new ServletContextHandler(contexts, RemoveJobServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        removeJob.addServlet(new ServletHolder(new RemoveJobServlet(jobMap)), "/*");


        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        //
        /// Cluster management
        //

        // Register a new slave on the master
        //
        ServletContextHandler registerSlave= new ServletContextHandler(contexts, RegisterSlaveServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        registerSlave.addServlet(new ServletHolder(new RegisterSlaveServlet(detections)), "/*");

        // Get list of registered slave servers
        //
        ServletContextHandler getSlaves= new ServletContextHandler(contexts, GetSlavesServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        getSlaves.addServlet(new ServletHolder(new GetSlavesServlet(detections)), "/*");


        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        //
        /// Resources
        //

        // Add export
        //
        ServletContextHandler addExport = new ServletContextHandler(contexts, AddExportServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        addExport.addServlet(new ServletHolder(new AddExportServlet(jobMap, transformationMap)), "/*");

        // Get next value from a slave sequence
        //
        ServletContextHandler nextSequence = new ServletContextHandler(contexts, NextSequenceValueServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        nextSequence.addServlet(new ServletHolder(new NextSequenceValueServlet(transformationMap)), "/*");


        server.setHandlers(new Handler[] { securityHandler, contexts });

        // Start execution
        createListeners();

        server.start();
    }*/

    String SERVICE_NAME = "Kettle";
    public void startServer() throws Exception {
        server = new Server();

        List<String> roles = new ArrayList<>();
        roles.add( Constraint.ANY_AUTH );

        // Set up the security handler, optionally with JAAS
        //
        ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();

        if ( System.getProperty( "loginmodulename" ) != null
                && System.getProperty( "java.security.auth.login.config" ) != null ) {
            JAASLoginService jaasLoginService = new JAASLoginService( SERVICE_NAME );
            jaasLoginService.setLoginModuleName( System.getProperty( "loginmodulename" ) );
            if( jaasLoginService.getIdentityService() == null ) {
                jaasLoginService.setIdentityService( new DefaultIdentityService() );
            }
            securityHandler.setLoginService( jaasLoginService );
        } else {
            HashLoginService hashLoginService;
            SlaveServer slaveServer = transformationMap.getSlaveServerConfig().getSlaveServer();
            if ( !StrUtil.isEmpty( slaveServer.getPassword() ) ) {
                hashLoginService = new HashLoginService( SERVICE_NAME );
                UserStore userStore = new UserStore();
                userStore.addUser( slaveServer.getUsername(), new Password( slaveServer.getPassword() ),
                        new String[] { } );
                hashLoginService.setUserStore( userStore );
            } else {
                // See if there is a kettle.pwd file in the KETTLE_HOME directory:
                if ( StrUtil.isEmpty( passwordFile ) ) {
                    File homePwdFile = new File( Const.getKettleCartePasswordFile() );
                    if ( homePwdFile.exists() ) {
                        passwordFile = Const.getKettleCartePasswordFile();
                    } else {
                        passwordFile = Const.getKettleLocalCartePasswordFile();
                    }
                }
                hashLoginService = new HashLoginService( SERVICE_NAME, passwordFile ) {
                    @Override
                    protected String[] loadRoleInfo( UserPrincipal user ) {
                        List<String> newRoles = new ArrayList<>();
                        String[] roles = super.loadRoleInfo( user );
                        if ( null != roles ) {
                            Collections.addAll( newRoles, roles );
                        }
                        return newRoles.toArray( new String[ newRoles.size() ] );
                    }
                };
            }
            securityHandler.setLoginService( hashLoginService );
        }

        Constraint constraint = new Constraint();
        constraint.setName( Constraint.__BASIC_AUTH );
        constraint.setRoles( roles.toArray( new String[ roles.size() ] ) );
        constraint.setAuthenticate( true );

        ConstraintMapping constraintMapping = new ConstraintMapping();
        constraintMapping.setConstraint( constraint );
        constraintMapping.setPathSpec( "/*" );

        securityHandler.setConstraintMappings( new ConstraintMapping[] { constraintMapping } );
        securityHandler.setAuthenticator( new BasicAuthenticator() );


        // Add all the servlets defined in kettle-servlets.xml ...
        //
        ContextHandlerCollection contexts = new ContextHandlerCollection();

        // Root
        //
        ServletContextHandler
                root =
                new ServletContextHandler( contexts, GetRootServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS );
        GetRootServlet rootServlet = new GetRootServlet();
        rootServlet.setJettyMode( true );
        root.addServlet( new ServletHolder( rootServlet ), "/*" );

// Carte Status
        //
        ServletContextHandler status = new ServletContextHandler(contexts, GetStatusServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        status.addServlet(new ServletHolder(new GetStatusServlet(transformationMap, jobMap)), "/*");

        // Trans status
        //
        ServletContextHandler transStatus = new ServletContextHandler(contexts, GetTransStatusServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        transStatus.addServlet(new ServletHolder(new GetTransStatusServlet(transformationMap)), "/*");

        // Prepare execution
        //
        ServletContextHandler prepareExecution = new ServletContextHandler(contexts, PrepareExecutionTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        prepareExecution.addServlet(new ServletHolder(new PrepareExecutionTransServlet(transformationMap)), "/*");

        // Start execution
        //
        ServletContextHandler startExecution = new ServletContextHandler(contexts, StartExecutionTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        startExecution.addServlet(new ServletHolder(new StartExecutionTransServlet(transformationMap)), "/*");

        // Start transformation
        //
        ServletContextHandler startTrans = new ServletContextHandler(contexts, StartTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        startTrans.addServlet(new ServletHolder(new StartTransServlet(transformationMap)), "/*");

        // Pause/Resume transformation
        //
        ServletContextHandler pauseTrans = new ServletContextHandler(contexts, PauseTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        pauseTrans.addServlet(new ServletHolder(new PauseTransServlet(transformationMap)), "/*");

        // Stop transformation
        //
        ServletContextHandler stopTrans = new ServletContextHandler(contexts, StopTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        stopTrans.addServlet(new ServletHolder(new StopTransServlet(transformationMap)), "/*");

        // Transformation cleanup
        //
        ServletContextHandler cleanupTrans = new ServletContextHandler(contexts, CleanupTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        cleanupTrans.addServlet(new ServletHolder(new CleanupTransServlet(transformationMap)), "/*");

        // Add transformation
        //
        ServletContextHandler addTrans = new ServletContextHandler(contexts, AddTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        addTrans.addServlet(new ServletHolder(new AddTransServlet(transformationMap, socketRepository)), "/*");

        // Remove Transformation
        //
        ServletContextHandler removeTrans = new ServletContextHandler(contexts, RemoveTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        removeTrans.addServlet(new ServletHolder(new RemoveTransServlet(transformationMap)), "/*");

        // Step port reservation
        //
        ServletContextHandler getPort = new ServletContextHandler(contexts, AllocateServerSocketServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        getPort.addServlet(new ServletHolder(new AllocateServerSocketServlet(transformationMap)), "/*");

        // Port listing information
        //
        ServletContextHandler listPorts = new ServletContextHandler(contexts, ListServerSocketServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        listPorts.addServlet(new ServletHolder(new ListServerSocketServlet(transformationMap)), "/*");

        // Sniff transformation step
        //
        ServletContextHandler sniffStep = new ServletContextHandler(contexts, SniffStepServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        sniffStep.addServlet(new ServletHolder(new SniffStepServlet(transformationMap)), "/*");

        // execute transformation
        //
        ServletContextHandler executeTrans = new ServletContextHandler(contexts, ExecuteTransServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        executeTrans .addServlet(new ServletHolder(new ExecuteTransServlet(transformationMap)), "/*");

        // add repository
        //
        ServletContextHandler addRep = new ServletContextHandler(contexts, AddRepServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        addRep .addServlet(new ServletHolder(new AddRepServlet(transformationMap)), "/*");

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        //
        // The job handlers...
        //

        // Start job
        //
        ServletContextHandler startJob = new ServletContextHandler(contexts, StartJobServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        startJob.addServlet(new ServletHolder(new StartJobServlet(jobMap)), "/*");

        // Stop transformation
        //
        ServletContextHandler stopJob = new ServletContextHandler(contexts, StopJobServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        stopJob.addServlet(new ServletHolder(new StopJobServlet(jobMap)), "/*");

        // Trans status
        //
        ServletContextHandler jobStatus = new ServletContextHandler(contexts, GetJobStatusServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        jobStatus.addServlet(new ServletHolder(new GetJobStatusServlet(jobMap)), "/*");

        // Add job
        //
        ServletContextHandler addJob= new ServletContextHandler(contexts, AddJobServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        addJob.addServlet(new ServletHolder(new AddJobServlet(jobMap, socketRepository)), "/*");

        // Remove Job
        //
        ServletContextHandler removeJob = new ServletContextHandler(contexts, RemoveJobServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        removeJob.addServlet(new ServletHolder(new RemoveJobServlet(jobMap)), "/*");


        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        //
        /// Cluster management
        //

        // Register a new slave on the master
        //
        ServletContextHandler registerSlave= new ServletContextHandler(contexts, RegisterSlaveServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        registerSlave.addServlet(new ServletHolder(new RegisterSlaveServlet(detections)), "/*");

        // Get list of registered slave servers
        //
        ServletContextHandler getSlaves= new ServletContextHandler(contexts, GetSlavesServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        getSlaves.addServlet(new ServletHolder(new GetSlavesServlet(detections)), "/*");



        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        //
        /// Resources
        //

        // Add export
        //
        ServletContextHandler addExport = new ServletContextHandler(contexts, AddExportServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        addExport.addServlet(new ServletHolder(new AddExportServlet(jobMap, transformationMap)), "/*");

        // Get next value from a slave sequence
        //
        ServletContextHandler nextSequence = new ServletContextHandler(contexts, NextSequenceValueServlet.CONTEXT_PATH, ServletContextHandler.SESSIONS);
        nextSequence.addServlet(new ServletHolder(new NextSequenceValueServlet(transformationMap)), "/*");


        // setup jersey (REST)
        ServletHolder jerseyServletHolder = new ServletHolder( ServletContainer.class );
        jerseyServletHolder.setInitParameter( "com.sun.jersey.config.property.resourceConfigClass",
                "com.sun.jersey.api.core.PackagesResourceConfig" );
        jerseyServletHolder.setInitParameter( "com.sun.jersey.config.property.packages", "org.pentaho.di.www.jaxrs" );
        root.addServlet( jerseyServletHolder, "/api/*" );

        // setup static resource serving
        // ResourceHandler mobileResourceHandler = new ResourceHandler();
        // mobileResourceHandler.setWelcomeFiles(new String[]{"index.html"});
        // mobileResourceHandler.setResourceBase(getClass().getClassLoader().
        // getResource("org/pentaho/di/www/mobile").toExternalForm());
        // ServletContextHandler mobileContext = new ServletContextHandler(contexts, "/mobile", ServletContextHandler.SESSIONS);
        // mobileContext.setHandler(mobileResourceHandler);

        // Allow png files to be shown for transformations and jobs...
        //
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setResourceBase( "temp" );
        // add all handlers/contexts to server

        // set up static servlet
        ServletHolder staticHolder = new ServletHolder( "static", DefaultServlet.class );
        // resourceBase maps to the path relative to where carte is started
        staticHolder.setInitParameter( "resourceBase", "./static/" );
        staticHolder.setInitParameter( "dirAllowed", "true" );
        staticHolder.setInitParameter( "pathInfoOnly", "true" );
        root.addServlet( staticHolder, "/static/*" );

        HandlerList handlers = new HandlerList();
        handlers.setHandlers( new Handler[] { resourceHandler, contexts,root } );
        securityHandler.setHandler( handlers );

        server.setHandler( securityHandler );

        //ExtensionPointHandler.callExtensionPoint( log, KettleExtensionPoint.BeforeCarteStartup.id, server );

        // Start execution
        createListeners();

        server.start();
    }

    public void stopServer() {
		try {
			if (server != null) {
			  
			  // Stop the monitoring timer
			  //
			  if (slaveMonitoringTimer!=null) {
			    slaveMonitoringTimer.cancel();
			    slaveMonitoringTimer = null;
			  }
			  
				// Clean up all the server sockets...
				//
				socketRepository.closeAll();

				// Stop the server...
				//
				server.stop();
			}
		} catch (Exception e) {
			log.logError(BaseMessages.getString(PKG, "WebServer.Error.FailedToStop.Title"), BaseMessages.getString(PKG, "WebServer.Error.FailedToStop.Msg", "" + e));
		}
	}

    private void createListeners()
    {
        ServerConnector connector = new ServerConnector(server,new HttpConnectionFactory());
        connector.setPort(port);
        connector.setHost(hostname);
        connector.setSoLingerTime(1000);
        connector.setName(BaseMessages.getString(PKG, "WebServer.Log.KettleHTTPListener",hostname));
        log.logBasic(BaseMessages.getString(PKG, "WebServer.Log.CreateListener",hostname,""+port));

        server.setConnectors( new Connector[] { connector });
    }

    /**
     * @return the hostname
     */
    public String getHostname()
    {
        return hostname;
    }

    /**
     * @param hostname the hostname to set
     */
    public void setHostname(String hostname)
    {
        this.hostname = hostname;
    }

	/**
	 * @return the slave server detections
	 */
	public List<SlaveServerDetection> getDetections() {
		return detections;
	}

	/**
	 * This method registers a timer to check up on all the registered slave servers every X seconds.<br>
	 */
    private void startSlaveMonitoring() {
  		slaveMonitoringTimer = new Timer("WebServer Timer");
  		TimerTask timerTask = new TimerTask() {
		
			public void run() {
				for (SlaveServerDetection slaveServerDetection : detections) {
					SlaveServer slaveServer = slaveServerDetection.getSlaveServer();
					
					// See if we can get a status...
					//
					try {
						// TODO: consider making this lighter or retaining more information...
						slaveServer.getStatus(); // throws the exception
						slaveServerDetection.setActive(true);
						slaveServerDetection.setLastActiveDate(new Date());
					} catch(Exception e) {
						slaveServerDetection.setActive(false);
						slaveServerDetection.setLastInactiveDate(new Date());
						
						// TODO: kick it out after a configurable period of time...
					}
				}
			}
		};
		slaveMonitoringTimer.schedule(timerTask, 20000, 20000);
	}

	/**
	 * @return the socketRepository
	 */
	public SocketRepository getSocketRepository() {
		return socketRepository;
	}

	/**
	 * @param socketRepository the socketRepository to set
	 */
	public void setSocketRepository(SocketRepository socketRepository) {
		this.socketRepository = socketRepository;
	}
}

