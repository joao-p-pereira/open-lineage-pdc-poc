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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.database.PostgreSQLDatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.step.BaseDatabaseStepMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.databaselookup.DatabaseLookupMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

/**
 * Read Json files, parse them and convert them to rows and writes these to one or more output streams.
 *
 * @author schintalapati
 */
@ExtensionPoint(
        description = "Transformation Runtime open lineage generator",
        extensionPointId = "TransformationStartThreads",
        id = "transRuntimeLineage" )
public class LineageJsonGenerator implements TransListener, ExtensionPointInterface {
  private Logger logger = LoggerFactory.getLogger( LineageJsonGenerator.class );
  StepMeta stepMeta=new StepMeta();

  public LineageJsonGenerator() {
    super();
  }

  @Override
  public void transStarted(Trans trans) throws KettleException {
    logger.info("Entered Start");
  }

  @Override
  public void transActive(Trans trans) {
    logger.info("Entered active");
  }

  @Override
  public void transFinished(Trans trans) throws KettleException {
    createLineageJson(trans);
  }

  private void createLineageJson( Trans trans ) throws KettleException {
    JSONArray lineageJson = new JSONArray();
    for( RowSet row:trans.rowsets ){
      JSONObject runObject=new JSONObject();
      completeEventJson(trans, lineageJson, row, runObject);
      // startEventJson(trans, lineageJson, row, runObject);
      startEventJson(trans, lineageJson, row, runObject);
      runObject.put( "runId",trans.getContainerObjectId() );
    }
    writeToFile( trans, lineageJson.toString() );
  }

  private void completeEventJson( Trans trans, JSONArray output, RowSet row, JSONObject runObject ) {
    JSONObject jsonObjectComplete=new JSONObject();
    jsonObjectComplete.put("eventType","COMPLETE");
    jsonObjectComplete.put("eventTime", Instant.now().toString());
    jsonObjectComplete.put("run", runObject);
    JSONObject completeJobObject=new JSONObject();
    completeJobObject.put("namespace", trans.getName());
    jsonObjectComplete.put("job",completeJobObject);
    completeJobObject.put( "name", row.getDestinationStepName() );
    output.add( jsonObjectComplete );
  }

  private void startEventJson(Trans trans, JSONArray output, RowSet row, JSONObject runObject) throws KettleStepException, KettleDatabaseException {

    JSONObject jsonObjectStart=new JSONObject();
    jsonObjectStart.put("eventType","START");
    jsonObjectStart.put("eventTime",Instant.now().toString() );
    jsonObjectStart.put("run", runObject);

    jobJson(trans, row, jsonObjectStart);

    JSONArray inputArray = getInputArrayJson(row,trans);
    jsonObjectStart.put( "inputs",inputArray );
    JSONArray outputArray = getOutputArrayJson(row,trans);
    jsonObjectStart.put( "outputs",outputArray );
    jsonObjectStart.put( "producer","" );
    output.add( jsonObjectStart );
  }

  private JSONArray getOutputArrayJson( RowSet row, Trans trans ) throws KettleStepException, KettleDatabaseException {
    JSONArray outputArray = new JSONArray();
    JSONObject output = new JSONObject();
    JSONObject outputFacetJson = new JSONObject();
    JSONObject dataSource = new JSONObject();
    dataSource.put("_producer","");
    dataSource.put("_schemaURL","");
    dataSource.put("name","");
    dataSource.put("uri","");
    outputFacetJson.put("datasource",dataSource);
    outputFacetJson.put("schema",null );
    outputFacetJson.put("sql",null);
    outputFacetJson.put("documentation",null);

    for(int i=0;i<trans.getSteps().size();i++) {

      if (trans.getSteps().get(i).stepname.equals(row.getOriginStepName())) {
        if (trans.getSteps().get(i).stepMeta.getStepMetaInterface() instanceof BaseDatabaseStepMeta) {
          DatabaseMeta dbmeta = ((BaseDatabaseStepMeta) trans.getSteps().get(i).stepMeta.getStepMetaInterface()).getDatabaseMeta();
          String dbName = dbmeta.getPluginId();
          String uri = dbmeta.getURL();
          dataSource.put("name", dbName);
          dataSource.put("uri",uri);
        }
      }
    }

    output.put("facets",outputFacetJson);

    output.put("name",row.getDestinationStepName());
    output.put("namespace",trans.getName());
    outputArray.add( output );
    return outputArray;
  }

  private JSONArray getInputArrayJson( RowSet row,Trans trans) throws KettleDatabaseException {
    JSONArray inputArray = new JSONArray();
    JSONObject input = new JSONObject();
    JSONObject inputFacetJson = new JSONObject();
    JSONObject schemaJson = new JSONObject();
    JSONArray fieldsArray = new JSONArray();
    JSONObject dataSource = new JSONObject();
    fieldJsonCreation(row, fieldsArray);
    schemaJson.put("fields",fieldsArray);
    inputFacetJson.put("dataSource",dataSource);
    inputFacetJson.put("schema",schemaJson);
    inputFacetJson.put("sql",null);
    inputFacetJson.put("filepath", Utils.isEmpty( trans.getTransMeta().getFilename() ) ? "NONE" : trans.getTransMeta().getFilename() );
    inputFacetJson.put("documentation",null);
    for(int i=0;i<trans.getSteps().size();i++) {

      if (trans.getSteps().get(i).stepname.equals(row.getOriginStepName())) {
        if (trans.getSteps().get(i).stepMeta.getStepMetaInterface() instanceof BaseDatabaseStepMeta) {
          DatabaseMeta dbmeta = ((BaseDatabaseStepMeta) trans.getSteps().get(i).stepMeta.getStepMetaInterface()).getDatabaseMeta();
          String dbName = dbmeta.getPluginId();
          String uri = dbmeta.getURL();
          dataSource.put("name", dbName);
          dataSource.put("uri",uri);
        }
      }
    }
    input.put("facets",inputFacetJson);
    input.put("namespace",trans.getName());
    input.put("name",row.getOriginStepName());
    inputArray.add( input );
    return inputArray;
  }

  private void jobJson( Trans trans, RowSet row, JSONObject jsonObjectStart  ) {
    JSONObject startJobObject=new JSONObject();

    startJobObject.put( "namespace", trans.getName() );
    startJobObject.put( "name", row.getDestinationStepName() );

    JSONObject facetJson = new JSONObject();
    facetJson.put("schema",null);
    facetJson.put("dataSource",null);


    JSONObject documentationJson = new JSONObject();
    documentationJson.put("_producer","");
    documentationJson.put("_schemaURL","");
    documentationJson.put("description","");
    facetJson.put("documentation",documentationJson);

    JSONObject sqlJson = new JSONObject();
    sqlJson.put("_producer","");
    sqlJson.put("_schemaURL","");
    sqlJson.put("description","");
    facetJson.put("sql",sqlJson);

    startJobObject.put( "facets",facetJson );
    jsonObjectStart.put("job",startJobObject);
  }

  private void fieldJsonCreation( RowSet row, JSONArray fieldsArray ) {
    if(row.getRowMeta()!=null) {
      for (ValueMetaInterface col : row.getRowMeta().getValueMetaList()) {
        JSONObject field = new JSONObject();
        field.put("name", col.getName());
        field.put("type", ValueMetaInterface.getTypeDescription(col.getType()));
        field.put("description", null);
        fieldsArray.add(field);
      }
    }
  }

  private void writeToFile( Trans trans, String lineageJson ) throws KettleException {
    String filePath = getFilename( trans.getTransMeta() );
    try {
      String path = KettleVFS.getFileObject( filePath ).getParent().toString()+"/"+trans.getName()+"-lineage.json";
      OutputStream fos = KettleVFS.getOutputStream( path, trans.getTransMeta(), false );
      fos.write( lineageJson.getBytes() );
      logger.info( "Open Lineage json written to: " + path );
      fos.flush();
    } catch ( IOException e ) {
      logger.error( "Failed while creating file" );
      new KettleException( e );
    }
  }

  private String getFilename(TransMeta transMeta) {
    if ( transMeta != null ) {
      String filename = transMeta.getFilename();
      if ( filename == null ) {
        filename = transMeta.getPathAndName();
        if ( transMeta.getDefaultExtension() != null ) {
          filename = filename + "." + transMeta.getDefaultExtension();
        }
      }
      return filename;
    }
    return null;
  }

  @Override
  public void callExtensionPoint(LogChannelInterface log, Object o) throws KettleException {
    if ( o != null && o instanceof Trans ) {
      Trans trans = ((Trans) o);
      if (trans.isPreview() /* based on some flag */) {
        return;
      }
      trans.addTransListener( this );
    }
  }
}
