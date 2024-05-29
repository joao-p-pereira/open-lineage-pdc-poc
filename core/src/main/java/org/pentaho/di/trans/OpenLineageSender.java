package org.pentaho.di.trans;

import io.openlineage.client.OpenLineage;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.ArrayList;
import java.util.List;

public class OpenLineageSender implements IOpenLineageSender {

  List<IOpenLineageWriter> senders;

  OpenLineageSender( OpenLineageConfig olConfig, LogChannelInterface log ) {
    senders = new ArrayList<>();
    for ( OpenLineageSenderMode mode : olConfig.getOpenLineageModes() ) {
      switch ( mode ) {
        case CONSOLE:
          senders.add( new OpenLineageConsoleWriter( log ) );
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
    senders.forEach( consumer -> consumer.emit( event ) );
  }

  public void emit( OpenLineage.JobEvent event ) {
    senders.forEach( consumer -> consumer.emit( event ) );
  }

  public void emit( OpenLineage.DatasetEvent event ) {
    senders.forEach( consumer -> consumer.emit( event ) );
  }
}
