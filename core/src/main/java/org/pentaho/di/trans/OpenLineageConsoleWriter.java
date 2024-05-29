package org.pentaho.di.trans;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

public class OpenLineageConsoleWriter implements IOpenLineageWriter {

  LogChannelInterface log;

  OpenLineageConsoleWriter( LogChannelInterface log ) {
    this.log = log;
  }

  @Override public void emit( OpenLineage.RunEvent event ) {
    logEvent( OpenLineageClientUtils.toJson( event ) );
  }

  @Override public void emit( OpenLineage.JobEvent event ) {
    logEvent( OpenLineageClientUtils.toJson( event ) );
  }

  @Override public void emit( OpenLineage.DatasetEvent event ) {
    logEvent( OpenLineageClientUtils.toJson( event ) );
  }

  private void logEvent( String event ) {
    //TODO Maybe pretty print
    log.logBasic( event );
  }
}
