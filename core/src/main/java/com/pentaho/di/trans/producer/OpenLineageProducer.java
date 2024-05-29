package com.pentaho.di.trans.producer;

import com.pentaho.di.trans.config.OpenLineageConfig;
import io.openlineage.client.OpenLineage;
import org.pentaho.di.core.logging.LogChannelInterface;

import java.util.ArrayList;
import java.util.List;

public class OpenLineageProducer implements IOpenLineageProducer {

  List<IOpenLineageSender> senders;

  public OpenLineageProducer( OpenLineageConfig olConfig, LogChannelInterface log ) {
    senders = new ArrayList<>();
    for ( OpenLineageSenderMode mode : olConfig.getOpenLineageModes() ) {
      switch ( mode ) {
        case CONSOLE:
          senders.add( new OpenLineageConsoleSender( log ) );
          break;
        case HTTP:
          senders.add( new OpenLineageHTTPSender() );
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
