package org.pentaho.di.trans;

import io.openlineage.client.OpenLineage;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class OpenLineageSender implements IOpenLineageSender {

  List<IOpenLineageWriter> senders;

  OpenLineageSender( OpenLineageConfig olconfig ){
    senders = new ArrayList<>();
    for ( OpenLineageSenderMode mode : olconfig.getOpenLineageModes() ) {
      switch ( mode ) {
        case CONSOLE:
          senders.add( new OpenLineageConsoleWriter() );
          break;
        case HTTP:
          senders.add( new OpenLineageHTTPWriter() );
          break;
        // Add more cases as needed
        default:
          break;
      }
    }
  }

  public void emit( OpenLineage.RunEvent event ) {
    senders.forEach(consumer -> consumer.emit( event ));
  }

  public void emit( OpenLineage.JobEvent event ) {
    senders.forEach(consumer -> consumer.emit( event ));
  }

  public void emit( OpenLineage.DatasetEvent event ) {
    senders.forEach(consumer -> consumer.emit( event ));
  }
}
