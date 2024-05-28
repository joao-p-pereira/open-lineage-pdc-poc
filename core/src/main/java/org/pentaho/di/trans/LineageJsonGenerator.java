/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.trans;


import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ExtensionPoint(
  description = "Open lineage generator",
  extensionPointId = "TransformationStartThreads",
  id = "transOpenLineage" )
public class LineageJsonGenerator implements TransListener, ExtensionPointInterface {
  private final Logger LOGGER = LoggerFactory.getLogger( LineageJsonGenerator.class );

  public LineageJsonGenerator() {
    super();
  }

  @Override
  public void transStarted( Trans trans ) throws KettleException {

    var config = OpenLineageConfig.getInstance();
    // Create the configuration here, maybe from properties
    // Create a component responsible for sending open lineage information based on a conf passed
    // Possible create have two Options: HTTP and Console (HttpTransport,ConsoleTransport)
    // Send Start event
    LOGGER.info( "Entered Start" );
    LogChannelInterface log = new LogChannel( "APP_NAME" );
    log.logBasic( "basic logging" );


  }

  @Override
  public void transActive( Trans trans ) {
    // Do nothing... or send running event?
    LOGGER.info( "Entered active" );

  }

  @Override
  public void transFinished( Trans trans ) throws KettleException {
    // Create the open lineage complete/fail/abort event in here
    // Pass it to the
    LOGGER.info( "Entered Finished" );
    createLineageJson( trans );
    //Send Lineage using the component created in transStarted
    //SendLineage(var lineage);
  }

  private void createLineageJson( Trans trans ) {

  }



  @Override
  public void callExtensionPoint( LogChannelInterface log, Object o ) throws KettleException {
    if ( o instanceof Trans ) {
      Trans trans = ( (Trans) o );
      if ( trans.isPreview() ) {
        return;
      }
      trans.addTransListener( this );
    }
  }
}
