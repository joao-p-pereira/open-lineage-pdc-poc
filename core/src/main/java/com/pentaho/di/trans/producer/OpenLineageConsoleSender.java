package com.pentaho.di.trans.producer;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import org.pentaho.di.core.logging.LogChannelInterface;

public class OpenLineageConsoleSender implements IOpenLineageSender {

  LogChannelInterface log;

  OpenLineageConsoleSender( LogChannelInterface log ) {
    this.log = log;
  }

  @Override public void emit( OpenLineage.RunEvent event ) {
    logEvent( "Open Lineage Run event: ".concat( OpenLineageClientUtils.toJson( event ) ) );
  }

  @Override public void emit( OpenLineage.JobEvent event ) {
    logEvent( "Open Lineage Job event: ".concat( OpenLineageClientUtils.toJson( event ) ) );
  }

  @Override public void emit( OpenLineage.DatasetEvent event ) {
    logEvent( "Open Lineage Dataset event: ".concat( OpenLineageClientUtils.toJson( event ) ) );
  }

  private void logEvent( String event ) {
    //TODO Maybe pretty print
    log.logBasic( event );
  }
}
