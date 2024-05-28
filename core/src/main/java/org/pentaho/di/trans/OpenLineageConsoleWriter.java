package org.pentaho.di.trans;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleConfig;
import io.openlineage.client.transports.ConsoleTransport;

import java.net.URI;

public class OpenLineageConsoleWriter implements IOpenLineageWriter {

  OpenLineageClient client;

  OpenLineageConsoleWriter() {
    OpenLineageClient client = new OpenLineageClient( new ConsoleTransport() );
  }

  @Override public void emit( OpenLineage.RunEvent event ) {
    client.emit( event );
  }
}
