package org.pentaho.di.trans;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleConfig;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class OpenLineageSender<T extends OpenLineage.BaseEvent> {

List<IOpenLineageWriter> senders;

  OpenLineageSender( OpenLineageConfig Olconfig ) throws URISyntaxException {
    senders = new ArrayList<>();
    senders.add( new OpenLineageConsoleWriter() );
   // HttpConfig config = new ConsoleConfig();
   // config.setUrl( new URI( "http://localhost:5000" ) );
   // var x = new OpenLineageClient( new HttpTransport( config ) );
  }

  public void emit(T event){
    if (event instanceof OpenLineage.RunEvent ){
      for ( var sender : senders ){
        sender.emit((OpenLineage.RunEvent)event);
      }
    }
  }
}
