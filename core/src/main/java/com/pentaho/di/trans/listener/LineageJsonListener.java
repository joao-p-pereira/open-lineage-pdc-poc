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

package com.pentaho.di.trans.listener;


import com.pentaho.di.trans.config.OpenLineageConfig;
import com.pentaho.di.trans.producer.OpenLineageProducer;
import io.openlineage.client.OpenLineage;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransListener;

import java.net.URI;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.UUID;


@ExtensionPoint(
  description = "Open lineage Plugin",
  extensionPointId = "TransformationStartThreads",
  id = "OpenLineageListener" )
public class LineageJsonListener implements TransListener, ExtensionPointInterface {
  //TODO when the plugin exists in the main branch change this from pdi-openlineage-plugin-ee to main
  protected static final String PRODUCER_URL =
    "https://github.com/pentaho/pdi-plugins-ee/tree/pdi-openlineage-plugin-ee/pdi-openlineage-plugin";
  LogChannelInterface log;
  OpenLineageProducer openLineageProducer;
  OpenLineageConfig config;

  public LineageJsonListener() {
    super();
  }

  @Override
  public void transStarted( Trans trans ) {
    log = new LogChannel( trans.getName() );
    log.logDetailed( "Calling transStarted for Open Lineage plugin" );

    config = OpenLineageConfig.getInstance();
    openLineageProducer = new OpenLineageProducer( config, log );

    openLineageProducer.emit( createRunEvent( trans, OpenLineage.RunEvent.EventType.START,
      trans.getCurrentDate().toInstant().atZone( ZoneId.systemDefault() ) ) );
  }

  private static OpenLineage.RunEvent createRunEvent( Trans trans, OpenLineage.RunEvent.EventType eventType,
                                                      ZonedDateTime eventTime ) {
    URI producer = URI.create( PRODUCER_URL );
    OpenLineage ol = new OpenLineage( producer );

    //TODO verify this, for some reason log channel id is used as transformation runID
    //https://github.com/pentaho/pentaho-kettle/blob/a07f8f272a7819cd1bdb4bcb4e6fde4602a84829/engine/src/main/java/org/pentaho/di/trans/Trans.java#L812
    UUID runId = UUID.fromString( trans.getLogChannelId() );

    OpenLineage.Run run = ol.newRun( runId, ol.newRunFacetsBuilder().build() );
    OpenLineage.Job job =
      ol.newJob( trans.getExecutingServer(), trans.getTransMeta().getFilename(), ol.newJobFacetsBuilder().build() );

    return ol.newRunEvent( eventTime, eventType, run, job, new ArrayList<>(),
      new ArrayList<>() );
  }

  @Override
  public void transActive( Trans trans ) {
  }

  @Override
  public void transFinished( Trans trans ) {
    // Create the open lineage complete/fail/abort event in here
    log.logDetailed( "Calling transFinished for OpenLineage plugin" );
    var eventType =
      trans.getErrors() > 0 ? OpenLineage.RunEvent.EventType.FAIL : OpenLineage.RunEvent.EventType.COMPLETE;
    openLineageProducer.emit( createRunEvent( trans, eventType,
      trans.getEndDate().toInstant().atZone( ZoneId.systemDefault() ) ) );
  }

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object o ) {
    if ( o instanceof Trans ) {
      Trans trans = ( (Trans) o );
      if ( trans.isPreview() ) {
        return;
      }
      trans.addTransListener( this );
    }
  }
}
