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


import io.openlineage.client.OpenLineage;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.UUID;


@ExtensionPoint(
  description = "Open lineage Plugin",
  extensionPointId = "TransformationStartThreads",
  id = "transOpenLineage" )
public class LineageJsonGenerator implements TransListener, ExtensionPointInterface {
  LogChannelInterface log = new LogChannel( "Open Lineage Plugin" );

  public LineageJsonGenerator() {
    super();
  }

  @Override
  public void transStarted( Trans trans ) throws KettleException {
    log.logDetailed("Calling transStarted for OpenLineage plugin");
    var config = OpenLineageConfig.getInstance();
    var openLineageSender = new OpenLineageSender(config);




    URI producer = URI.create( "Produced_by_wookies_team" );
    OpenLineage ol = new OpenLineage( producer );

    ZonedDateTime now = ZonedDateTime.now( ZoneOffset.UTC );
    UUID runId = UUID.randomUUID();

    OpenLineage.RunFacets runFacets =
      ol.newRunFacetsBuilder().nominalTime( ol.newNominalTimeRunFacet( now, now ) ).build();
    var run = ol.newRun( runId, runFacets );


    String name = "jobName";
    String namespace = "jobNamespace";
    OpenLineage.JobFacets jobFacets = ol.newJobFacetsBuilder().build();
    OpenLineage.Job job = ol.newJob( namespace, name, jobFacets );


    var runEvent = ol.newRunEvent( now, OpenLineage.RunEvent.EventType.START, run, job, new ArrayList<>(), new ArrayList<>() );

    openLineageSender.emit( runEvent );
    // Create the configuration here, maybe from properties
    // Create a component responsible for sending open lineage information based on a conf passed
    // Possible create have two Options: HTTP and Console (HttpTransport,ConsoleTransport)
    // Send Start event
    LogChannelInterface log = new LogChannel( "APP_NAME" );
    log.logBasic( "basic logging" );


  }

  @Override
  public void transActive( Trans trans ) {
    // Do nothing... or send running event?
    log.logBasic( "Entered active" );

  }

  @Override
  public void transFinished( Trans trans ) throws KettleException {
    // Create the open lineage complete/fail/abort event in here
    log.logBasic( "Entered Finished" );
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
