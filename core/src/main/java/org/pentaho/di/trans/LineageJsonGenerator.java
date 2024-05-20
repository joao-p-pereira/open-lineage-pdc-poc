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
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.step.StepMetaDataCombi;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ExtensionPoint(
  description = "Transformation Runtime open lineage generator",
  extensionPointId = "TransformationStartThreads",
  id = "transRuntimeLineage" )
public class LineageJsonGenerator implements TransListener, ExtensionPointInterface {
  private Logger logger = LoggerFactory.getLogger( LineageJsonGenerator.class );

  public LineageJsonGenerator() {
    super();
  }

  @Override
  public void transStarted( Trans trans ) throws KettleException {
    logger.info( "Entered Start" );
  }

  @Override
  public void transActive( Trans trans ) {
    logger.info( "Entered active" );
  }

  @Override
  public void transFinished( Trans trans ) throws KettleException {
    try {
      createLineageJson( trans );
    } catch ( URISyntaxException e ) {
      throw new RuntimeException( e );
    }
  }

  private void createLineageJson( Trans trans ) throws URISyntaxException {
    OpenLineageClient client = getOpenLineageClient();

    ZonedDateTime now = ZonedDateTime.now( ZoneOffset.UTC );
    URI producer = URI.create( "Produced_by_wookies_team" );// Dummy
    OpenLineage ol = new OpenLineage( producer );

    List<OpenLineage.OutputDataset> outputs = getOutputs( trans, ol );
    List<OpenLineage.InputDataset> inputs = getInputs( trans, ol );

    String job_namespace = "wookiesNamespace"; // should some kind of schedule id
    String job_name = trans.getName();
    OpenLineage.Run run = createRun( ol );
    OpenLineage.Job job = createJob( job_name, job_namespace, ol, trans );

    OpenLineage.RunEvent runStateUpdate =
      ol.newRunEvent( now, OpenLineage.RunEvent.EventType.START, run, job, inputs, outputs );
    OpenLineage.RunEvent runStateUpdateComplete =
      ol.newRunEvent( now, OpenLineage.RunEvent.EventType.COMPLETE, run, job, inputs, outputs );

    client.emit( runStateUpdate );
    client.emit( runStateUpdateComplete );

  }

  private static OpenLineageClient getOpenLineageClient() throws URISyntaxException {
    HttpConfig config = new HttpConfig();
    config.setUrl( new URI( "http://localhost:5000" ) );
    return new OpenLineageClient( new HttpTransport( config ) );
  }

  private OpenLineage.Run createRun( OpenLineage ol ) {
    ZonedDateTime now = ZonedDateTime.now( ZoneOffset.UTC );
    UUID runId = UUID.randomUUID();

    OpenLineage.RunFacets runFacets =
      ol.newRunFacetsBuilder().nominalTime( ol.newNominalTimeRunFacet( now, now ) ).build();
    return ol.newRun( runId, runFacets );
  }

  private OpenLineage.Job createJob( String jobName, String jobNamespace, OpenLineage ol, Trans trans ) {
    OpenLineage.SQLJobFacet sqlJobFacet = ol.newSQLJobFacet( getQuery( trans ) ); // Just to test the query on UI
    OpenLineage.JobFacets jobFacets = ol.newJobFacetsBuilder().sql( sqlJobFacet ).build();
    return ol.newJob( jobNamespace, jobName, jobFacets );
  }

  private static String getQuery( Trans trans ) {
    for ( StepMetaDataCombi step : trans.getSteps() ) {
      if ( step.stepMeta.getStepMetaInterface() instanceof TableInputMeta ) {
        TableInputMeta tMeta = ( (TableInputMeta) step.stepMeta.getStepMetaInterface() );
        return tMeta.getSQL();
      }
    }
    return "Query not found";
  }

  private List<OpenLineage.OutputDataset> getOutputs( Trans trans, OpenLineage ol ) {
    List<OpenLineage.OutputDataset> list = new ArrayList<>();

    for ( StepMetaDataCombi step : trans.getSteps() ) {
      if ( step.stepMeta.getStepMetaInterface() instanceof TableOutputMeta ) {
        createTableOutputDataset( ol, step, list );
      }
      if ( step.stepMeta.getStepMetaInterface() instanceof TextFileOutputMeta ) {
        createFileOutputDataset( ol, step, list );
      }
    }

    return list;
  }

  private static void createFileOutputDataset( OpenLineage ol, StepMetaDataCombi step,
                                               List<OpenLineage.OutputDataset> list ) {
    TextFileOutputMeta tMeta = ( (TextFileOutputMeta) step.stepMeta.getStepMetaInterface() );
    String[] parts = tMeta.getFileName().split( "\\\\" );
    // Just because it seems there is a issue with full path in Marquez
    String fName = parts[ parts.length - 1 ];
    // Dummy namespace or test, all files are localhost
    list.add( ol.newOutputDataset( "localhost" + ":" + "22", fName, null, null ) );
  }

  private static void createTableOutputDataset( OpenLineage ol, StepMetaDataCombi step,
                                                List<OpenLineage.OutputDataset> list ) {
    TableOutputMeta tMeta = ( (TableOutputMeta) step.stepMeta.getStepMetaInterface() );
    DatabaseMeta dbmeta = tMeta.getDatabaseMeta();

    String hostname = dbmeta.getHostname();
    String port = dbmeta.getDatabasePortNumberString();
    String databaseName = dbmeta.getDatabaseName();
    String schema = dbmeta.getUsername();
    String tableName = tMeta.getTableName();

    list.add(
      ol.newOutputDataset( hostname + ":" + port, databaseName + "." + schema + "." + tableName, null, null ) );
  }

  private List<OpenLineage.InputDataset> getInputs( Trans trans, OpenLineage ol ) {
    List<OpenLineage.InputDataset> list = new ArrayList<>();

    for ( StepMetaDataCombi step : trans.getSteps() ) {
      if ( step.stepMeta.getStepMetaInterface() instanceof TableInputMeta ) {
        createTableInputDataset( ol, step, list );
      }
    }

    return list;
  }

  private static void createTableInputDataset( OpenLineage ol, StepMetaDataCombi step,
                                               List<OpenLineage.InputDataset> list ) {
    TableInputMeta tMeta = ( (TableInputMeta) step.stepMeta.getStepMetaInterface() );
    DatabaseMeta dbmeta = tMeta.getDatabaseMeta();

    String hostname = dbmeta.getHostname();
    String port = dbmeta.getDatabasePortNumberString();
    String databaseName = dbmeta.getDatabaseName();
    String schema = dbmeta.getUsername();

    //Naive implementation...
    String regex = "(?i)(?<=from)\\s+(\\w+\\b)";
    Pattern pattern = Pattern.compile( regex, Pattern.MULTILINE );
    Matcher matcher = pattern.matcher( tMeta.getSQL() );
    while ( matcher.find() ) {
      String tableName = matcher.group( 1 );
      list.add(
        ol.newInputDataset( hostname + ":" + port, databaseName + "." + schema + "." + tableName, null, null ) );
    }
  }

  @Override
  public void callExtensionPoint( LogChannelInterface log, Object o ) throws KettleException {
    if ( o instanceof Trans ) {
      Trans trans = ( (Trans) o );
      if ( trans.isPreview() /* based on some flag */ ) {
        return;
      }
      trans.addTransListener( this );
    }
  }
}
