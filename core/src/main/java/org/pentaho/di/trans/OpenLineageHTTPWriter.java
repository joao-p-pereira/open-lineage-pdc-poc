package org.pentaho.di.trans;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;

import java.net.URI;

public class OpenLineageHTTPWriter implements IOpenLineageWriter {

  OpenLineageClient client;

  OpenLineageHTTPWriter() {
    HttpConfig config = new HttpConfig();
    config.setUrl( URI.create( "http://localhost:5000" ) );
    client = new OpenLineageClient( new HttpTransport( config ) );
  }

  @Override public void emit( OpenLineage.RunEvent event ) {
    client.emit( event );
  }

  @Override public void emit( OpenLineage.JobEvent event ) {
    client.emit( event );
  }

  @Override public void emit( OpenLineage.DatasetEvent event ) {
    client.emit( event );
  }
}
